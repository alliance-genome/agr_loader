import json, time, random
import logging, urllib, xmltodict
import os

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.gene_description import GeneDescription
from transactors import CSVTransactor, Neo4jTransactor
from genedescriptions.data_manager import DataManager
from genedescriptions.commons import DataType, Gene
from genedescriptions.precanned_modules import set_gene_ontology_module
from common import ContextInfo
from data_manager import DataFileManager

logger = logging.getLogger(__name__)


class GeneDescriptionsETL(ETL):

    GeneDescriptionsQuery = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
        MATCH (o:Gene) where o.primaryKey = row.genePrimaryKey 
        SET o.automatedGeneSynopsis = row.geneDescription
        """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        # create gene descriptions data manager load common data
        context_info = ContextInfo()
        data_manager = DataFileManager(context_info.config_file_location)
        go_onto_config = data_manager.get_config('GO')
        go_annot_config = data_manager.get_config('GOAnnot')
        data_providers = [sub.get_data_provider() for sub in go_annot_config.get_sub_type_objects()]
        go_annot_sub_dict = {sub.get_data_provider(): sub for sub in go_annot_config.get_sub_type_objects()}
        this_dir = os.path.split(__file__)[0]
        gd_config = GenedescConfigParser(os.path.join(this_dir, os.pardir, "config", "gene_descriptions.yml"))
        gd_data_manager = DataManager(do_relations=None, go_relations=["subClassOf", "BFO:0000050"])
        gd_data_manager.load_ontology_from_file(ontology_type=DataType.GO,
                                                ontology_url="file://" +
                                                             os.path.join(os.getcwd(),
                                                                          go_onto_config.get_single_filepath()),
                                                ontology_cache_path=os.path.join(os.getcwd(), "tmp", "gd_cache",
                                                                                 "go.obo"),
                                                config=gd_config)
        for data_provider in data_providers:
            gd_data_manager.load_associations_from_file(
                associations_type=DataType.GO, associations_url="file://" + os.path.join(
                    os.getcwd(), go_annot_sub_dict[data_provider].get_single_filepath()),
                associations_cache_path=os.path.join(os.getcwd(), "tmp", "gd_cache", "go_annot_" +
                                                     data_provider + ".gaf"),
                config=gd_config)
            commit_size = self.data_type_config.get_neo4j_commit_size()
            generators = self.get_generators(data_provider, gd_data_manager, gd_config)
            query_list = [
                [GeneDescriptionsETL.GeneDescriptionsQuery, commit_size, "genedescriptions_data_" +
                 data_provider + ".csv"], ]
            
            query_and_file_list = self.process_query_params(query_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, data_provider, gd_data_manager, gd_config):
        query = "match (g:Gene) where g.dataProvider = {parameter}"
        return_set = Neo4jHelper.run_single_parameter_query(query, data_provider)
        descriptions = []
        for record in return_set:
            gene = Gene(id=record["g.primaryKey"], name=record["g.symbol"], dead=False, pseudo=False)
            gene_desc = GeneDescription(gene_id=gene.id, gene_name=gene.name, add_gene_name=False)
            set_gene_ontology_module(dm=gd_data_manager, conf_parser=gd_config, gene_desc=gene_desc, gene=gene)
            descriptions.append(gene_desc.description)
        yield [descriptions]
