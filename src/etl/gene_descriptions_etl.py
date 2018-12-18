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
        this_dir = os.path.split(__file__)[0]
        gd_config = GenedescConfigParser(os.path.join(this_dir, os.pardir, "config", "gene_descriptions.yml"))
        gd_data_manager = DataManager(do_relations=None, go_relations=["subClassOf", "BFO:0000050"])
        gd_data_manager.load_ontology_from_file(ontology_type=DataType.GO,
                                                ontology_url=self.data_type_config.get_single_filepath(),
                                                ontology_cache_path=os.path.join(this_dir, "gd_cache", "go.obo"),
                                                config=gd_config)
        for sub_type in self.data_type_config.get_sub_type_objects():
            data_provider = sub_type.get_data_provider()

            commit_size = self.data_type_config.get_neo4j_commit_size()
            generators = self.get_generators(data_provider, gd_data_manager, gd_config)
            query_list = [
                [GeneDescriptionsETL.GeneDescriptionsQuery, commit_size, "genedescriptions_data_" +
                 sub_type.get_data_provider() + ".csv"], ]
            
            query_and_file_list = self.process_query_params(query_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)

    @staticmethod
    def get_generators(data_provider, gd_data_manager, gd_config):
        query = "match (g:Gene) where g.dataProvider = {parameter}"
        return_set = Neo4jHelper.run_single_parameter_query(query, data_provider)
        descriptions = []
        for record in return_set:
            gene = Gene(id=record["g.primaryKey"], name=record["g.symbol"], dead=False, pseudo=False)
            gene_desc = GeneDescription(gene_id=gene.id, gene_name=gene.name, add_gene_name=False)
            set_gene_ontology_module(dm=gd_data_manager, conf_parser=gd_config, gene_desc=gene_desc, gene=gene)
            descriptions.append(gene_desc.description)
        yield [descriptions]
