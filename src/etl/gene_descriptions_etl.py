import json, time, random
import logging, urllib, xmltodict
import os

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.gene_description import GeneDescription
from ontobio import AssociationSetFactory
from transactors import CSVTransactor, Neo4jTransactor
from genedescriptions.data_manager import DataManager
from genedescriptions.commons import DataType, Gene
from genedescriptions.precanned_modules import set_gene_ontology_module, set_disease_module
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

    GetAllGenesQuery = """
        
        MATCH (g:Gene) where g.dataProvider = {parameter} 
        RETURN g.primaryKey, g.symbol
        """

    GetGeneDiseaseAnnotQuery = """
    
        MATCH (d:DOTerm)-[r {dataProvider: {parameter}}]-(g:Gene)-[:ASSOCIATION]->(dga:Association:DiseaseEntityJoin)-[:ASSOCIATION]->(d) 
        MATCH (dga)-->(e:EvidenceCode)
        RETURN DISTINCT g.primaryKey AS geneId, g.symbol AS geneSymbol, d.primaryKey AS DOId, e.primaryKey AS ECode, type(r) AS relType
        """

    GetFeatureDiseaseAnnotQuery = """

        MATCH (d:DOTerm)-[r {dataProvider: {parameter}}]-(f:Feature)-[:ASSOCIATION]->(dga:Association:DiseaseEntityJoin)-[:ASSOCIATION]->(d) 
        MATCH (dga)-->(e:EvidenceCode)
        RETURN DISTINCT f.primaryKey AS geneId, f.symbol AS geneSymbol, d.primaryKey as DOId, e.primaryKey AS ECode, type(r) AS relType
        """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        # create gene descriptions data manager and load common data
        context_info = ContextInfo()
        data_manager = DataFileManager(context_info.config_file_location)
        go_onto_config = data_manager.get_config('GO')
        go_annot_config = data_manager.get_config('GOAnnot')
        do_onto_config = data_manager.get_config('DO')
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
        gd_data_manager.load_ontology_from_file(ontology_type=DataType.DO,
                                                ontology_url="file://" +
                                                             os.path.join(os.getcwd(),
                                                                          do_onto_config.get_single_filepath()),
                                                ontology_cache_path=os.path.join(os.getcwd(), "tmp", "gd_cache",
                                                                                 "do.obo"),
                                                config=gd_config)
        # generate descriptions for each MOD
        for prvdr in [sub_type.get_data_provider() for sub_type in self.data_type_config.get_sub_type_objects()]:
            gd_data_manager.load_associations_from_file(
                associations_type=DataType.GO, associations_url="file://" + os.path.join(
                    os.getcwd(), "tmp", go_annot_sub_dict[prvdr].file_to_download),
                associations_cache_path=os.path.join(os.getcwd(), "tmp", "gd_cache", "go_annot_" + prvdr + ".gaf.gz"),
                config=gd_config)
            gd_data_manager.set_associations(associations_type=DataType.DO,
                                             associations=self.get_disease_annotations_from_db(
                                                 data_provider=prvdr, gd_data_manager=gd_data_manager),
                                             config=gd_config)
            commit_size = self.data_type_config.get_neo4j_commit_size()
            generators = self.get_generators(prvdr, gd_data_manager, gd_config)
            query_list = [
                [GeneDescriptionsETL.GeneDescriptionsQuery, commit_size, "genedescriptions_data_" + prvdr + ".csv"], ]
            query_and_file_list = self.process_query_params(query_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, data_provider, gd_data_manager, gd_config):
        return_set = Neo4jHelper.run_single_parameter_query(GeneDescriptionsETL.GetAllGenesQuery, data_provider)
        descriptions = []
        for record in return_set:
            gene = Gene(id=record["g.primaryKey"], name=record["g.symbol"], dead=False, pseudo=False)
            gene_desc = GeneDescription(gene_id=gene.id, gene_name=gene.name, add_gene_name=False)
            set_gene_ontology_module(dm=gd_data_manager, conf_parser=gd_config, gene_desc=gene_desc, gene=gene)
            set_disease_module(df=gd_data_manager, conf_parser=gd_config, gene_desc=gene_desc, gene=gene)
            descriptions.append({
                "genePrimaryKey": gene_desc.gene_id,
                "geneDescription": gene_desc.description
            })
        yield [descriptions]

    @staticmethod
    def get_disease_annotations_from_db(data_provider, gd_data_manager):

        def get_disease_annotation(gene_id, gene_symbol, do_term_id, ecode, prvdr):
            return {"source_line": "",
                    "subject": {
                        "id": gene_id,
                        "label": gene_symbol,
                        "type": "gene",
                        "fullname": "",
                        "synonyms": [],
                        "taxon": {"id": ""}
                    },
                    "object": {
                        "id": do_term_id,
                        "taxon": ""
                    },
                    "qualifiers": "",
                    "aspect": "D",
                    "relation": {"id": None},
                    "negated": False,
                    "evidence": {
                        "type": ecode,
                        "has_supporting_reference": "",
                        "with_support_from": [],
                        "provided_by": prvdr,
                        "date": None
                    }}

        def add_annotations(final_annotation_list, neo4j_annot_set):
            for annot in neo4j_annot_set:
                ecodes = [ecode[1:-1] for ecode in annot["ECode"][1:-1].split(", ")] if \
                    annot["relType"] == "IS_IMPLICATED_IN" else ["IEP"]
                for ecode in ecodes:
                    final_annotation_list.append(get_disease_annotation(annot["geneId"], annot["geneSymbol"],
                                                                        annot["DOId"], ecode, data_provider))

        annotations = []
        gene_annot_set = Neo4jHelper.run_single_parameter_query(GeneDescriptionsETL.GetGeneDiseaseAnnotQuery,
                                                                data_provider)
        add_annotations(annotations, gene_annot_set)
        feature_annot_set = Neo4jHelper.run_single_parameter_query(GeneDescriptionsETL.GetFeatureDiseaseAnnotQuery,
                                                                   data_provider)
        add_annotations(annotations, feature_annot_set)
        return AssociationSetFactory().create_from_assocs(assocs=annotations, ontology=gd_data_manager.do_ontology)
