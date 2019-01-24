import logging
import os
import datetime

from collections import defaultdict

import boto3
from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.descriptions_writer import DescriptionsWriter
from genedescriptions.gene_description import GeneDescription
from ontobio import AssociationSetFactory
from transactors import CSVTransactor, Neo4jTransactor
from genedescriptions.data_manager import DataManager
from genedescriptions.commons import DataType, Gene, Module
from genedescriptions.precanned_modules import set_gene_ontology_module, set_disease_module, \
    set_alliance_human_orthology_module
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

    GetAllGenesHumanQuery = """

        MATCH (g:Gene) where g.dataProvider = {parameter} AND g.primaryKey CONTAINS "HGNC:"
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

    GetFilteredHumanOrthologsQuery = """
        
        MATCH (g2)<-[orth:ORTHOLOGOUS]-(g:Gene)-[:ASSOCIATION]->(ogj:Association:OrthologyGeneJoin)-[:ASSOCIATION]->(g2:Gene)
        WHERE ogj.joinType = 'orthologous' AND g.dataProvider = {parameter} AND g2.taxonId ='NCBITaxon:9606' AND orth.strictFilter = 'True'
        MATCH (ogj)-[:MATCHED]->(oa:OrthoAlgorithm)
        RETURN g.primaryKey AS geneId, g2.primaryKey AS orthoId, g2.symbol AS orthoSymbol, g2.name AS orthoName, oa.name AS algorithm
        """

    GetDiseaseViaOrthologyQuery = """
    
        MATCH (disease:DOTerm)-[da:IS_IMPLICATED_IN]-(gene1:Gene)-[o:ORTHOLOGOUS]->(gene2:Gene)
        MATCH (ec:EvidenceCode)-[:EVIDENCE]-(dej:DiseaseEntityJoin)-[:EVIDENCE]->(p:Publication)
        WHERE o.strictFilter = 'True' AND gene1.taxonId ='NCBITaxon:9606' AND da.uuid = dej.primaryKey 
        AND NOT ec.primaryKey IN ["IEA", "ISS", "ISO"] AND gene2.dataProvider = {parameter}
        OPTIONAL MATCH (disease:DOTerm)-[da2:ASSOCIATION]-(gene2:Gene)-[ag:IS_ALLELE_OF]->(:Feature)-[da3:IS_IMPLICATED_IN]-(disease:DOTerm)
        WHERE da2 IS null  // filters relations that already exist
        AND da3 IS null // filter where allele already has disease association
        RETURN DISTINCT gene2.primaryKey AS geneId, gene2.symbol AS geneSymbol, disease.primaryKey AS DOId, p.primaryKey AS publicationId
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
        do_annot_config = data_manager.get_config('Disease')
        go_annot_sub_dict = {sub.get_data_provider(): sub for sub in go_annot_config.get_sub_type_objects()}
        do_annot_sub_dict = {sub.get_data_provider(): sub for sub in do_annot_config.get_sub_type_objects()}
        this_dir = os.path.split(__file__)[0]
        gd_config = GenedescConfigParser(os.path.join(this_dir, os.pardir, "config", "gene_descriptions.yml"))
        gd_data_manager = DataManager(do_relations=None, go_relations=["subClassOf", "BFO:0000050"])
        go_onto_url = "file://" + os.path.join(os.getcwd(), go_onto_config.get_single_filepath())
        gd_data_manager.load_ontology_from_file(ontology_type=DataType.GO, ontology_url=go_onto_url, config=gd_config,
                                                ontology_cache_path=os.path.join(os.getcwd(), "tmp", "gd_cache", "go.obo"))
        do_onto_url = "file://" + os.path.join(os.getcwd(), do_onto_config.get_single_filepath())
        gd_data_manager.load_ontology_from_file(ontology_type=DataType.DO, ontology_url=do_onto_url, config=gd_config,
                                                ontology_cache_path=os.path.join(os.getcwd(), "tmp", "gd_cache", "do.obo"))
        # generate descriptions for each MOD
        for prvdr in [sub_type.get_data_provider() for sub_type in self.data_type_config.get_sub_type_objects()]:
            json_desc_writer = DescriptionsWriter()
            go_annot_url = "file://" + os.path.join(os.getcwd(), "tmp", go_annot_sub_dict[prvdr].file_to_download)
            do_annot_url = "file://" + os.path.join(os.getcwd(), "tmp", do_annot_sub_dict[prvdr].file_to_download)
            gd_data_manager.load_associations_from_file(
                associations_type=DataType.GO, associations_url=go_annot_url,
                associations_cache_path=os.path.join(os.getcwd(), "tmp", "gd_cache", "go_annot_" + prvdr + ".gaf.gz"),
                config=gd_config)
            key_diseases = defaultdict(set)
            gd_data_manager.set_associations(
                associations_type=DataType.DO, associations=self.get_disease_annotations_from_db(
                    data_provider=prvdr, gd_data_manager=gd_data_manager, key_diseases=key_diseases), config=gd_config)
            commit_size = self.data_type_config.get_neo4j_commit_size()
            generators = self.get_generators(prvdr, gd_data_manager, gd_config, key_diseases, json_desc_writer)
            query_list = [
                [GeneDescriptionsETL.GeneDescriptionsQuery, commit_size, "genedescriptions_data_" + prvdr + ".csv"], ]
            query_and_file_list = self.process_query_params(query_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)
            self.save_descriptions_report_files(
                data_provider=prvdr, json_desc_writer=json_desc_writer, go_ontology_url=go_onto_url,
                go_association_url=go_annot_url, do_ontology_url=do_onto_url, do_association_url=do_annot_url)

    def get_generators(self, data_provider, gd_data_manager, gd_config, key_diseases, json_desc_writer):
        if data_provider == "Human":
            return_set = Neo4jHelper.run_single_parameter_query(GeneDescriptionsETL.GetAllGenesHumanQuery, "RGD")
        else:
            return_set = Neo4jHelper.run_single_parameter_query(GeneDescriptionsETL.GetAllGenesQuery, data_provider)
        descriptions = []
        best_orthologs = self.get_best_orthologs_from_db(data_provider=data_provider)
        for record in return_set:
            gene = Gene(id=record["g.primaryKey"], name=record["g.symbol"], dead=False, pseudo=False)
            gene_desc = GeneDescription(gene_id=gene.id, gene_name=gene.name, add_gene_name=False)
            set_gene_ontology_module(dm=gd_data_manager, conf_parser=gd_config, gene_desc=gene_desc, gene=gene)
            set_disease_module(df=gd_data_manager, conf_parser=gd_config, gene_desc=gene_desc, gene=gene,
                               orthologs_key_diseases=key_diseases[gene.id])
            if gene.id in best_orthologs:
                set_alliance_human_orthology_module(orthologs=best_orthologs[gene.id][0],
                                                    excluded_orthologs=best_orthologs[gene.id][1], gene_desc=gene_desc)
            if gene_desc.description != "":
                descriptions.append({
                    "genePrimaryKey": gene_desc.gene_id,
                    "geneDescription": gene_desc.description
                })
            json_desc_writer.add_gene_desc(gene_desc)
        yield [descriptions]

    @staticmethod
    def create_disease_annotation_record(gene_id, gene_symbol, do_term_id, ecode, prvdr):
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

    @staticmethod
    def add_annotations(final_annotation_list, neo4j_annot_set, data_provider):
        for annot in neo4j_annot_set:
            ecodes = [ecode[1:-1] for ecode in annot["ECode"][1:-1].split(", ")] if \
                annot["relType"] == "IS_IMPLICATED_IN" else ["IEP"]
            for ecode in ecodes:
                final_annotation_list.append(GeneDescriptionsETL.create_disease_annotation_record(
                    annot["geneId"], annot["geneSymbol"], annot["DOId"], ecode, data_provider))

    @staticmethod
    def get_disease_annotations_from_db(data_provider, gd_data_manager, key_diseases):
        annotations = []
        gene_annot_set = Neo4jHelper.run_single_parameter_query(GeneDescriptionsETL.GetGeneDiseaseAnnotQuery,
                                                                data_provider)
        GeneDescriptionsETL.add_annotations(annotations, gene_annot_set, data_provider)
        feature_annot_set = Neo4jHelper.run_single_parameter_query(GeneDescriptionsETL.GetFeatureDiseaseAnnotQuery,
                                                                   data_provider)
        GeneDescriptionsETL.add_annotations(annotations, feature_annot_set, data_provider)
        disease_via_orth_records = Neo4jHelper.run_single_parameter_query(
            GeneDescriptionsETL.GetDiseaseViaOrthologyQuery, data_provider)
        for orth_annot in disease_via_orth_records:
            annotations.append(GeneDescriptionsETL.create_disease_annotation_record(
                gene_id=orth_annot["geneId"], gene_symbol=orth_annot["geneSymbol"], do_term_id=orth_annot["DOId"],
                ecode="DVO", prvdr=data_provider))
            if orth_annot["publicationId"] == "RGD:7240710":
                key_diseases[orth_annot["geneId"]].add(orth_annot["DOId"])
        return AssociationSetFactory().create_from_assocs(assocs=annotations, ontology=gd_data_manager.do_ontology)

    @staticmethod
    def get_best_orthologs_from_db(data_provider):
        orthologs_set = Neo4jHelper.run_single_parameter_query(GeneDescriptionsETL.GetFilteredHumanOrthologsQuery,
                                                               data_provider)
        genes_orthologs_algos = defaultdict(lambda: defaultdict(int))
        best_orthologs = {}
        orthologs_info = {}
        for ortholog_algo in orthologs_set:
            genes_orthologs_algos[ortholog_algo["geneId"]][ortholog_algo["orthoId"]] += 1
            if ortholog_algo["orthoId"] not in orthologs_info:
                orthologs_info[ortholog_algo["orthoId"]] = (ortholog_algo["orthoSymbol"], ortholog_algo["orthoName"])
        for gene_id in genes_orthologs_algos.keys():
            best_orthologs[gene_id] = [[[ortholog_id, orthologs_info[ortholog_id][0], orthologs_info[ortholog_id][1]]
                                        for ortholog_id in genes_orthologs_algos[gene_id].keys() if
                                        genes_orthologs_algos[gene_id][ortholog_id] ==
                                        max(genes_orthologs_algos[gene_id].values())], False]
            best_orthologs[gene_id][-1] = len(best_orthologs[gene_id][0]) != len(genes_orthologs_algos[gene_id].keys())
        return best_orthologs

    @staticmethod
    def save_descriptions_report_files(data_provider, json_desc_writer, go_ontology_url, go_association_url,
                                       do_ontology_url, do_association_url):
        if "GENERATE_REPORTS" in os.environ and (os.environ["GENERATE_REPORTS"] == "True"
                                                 or os.environ["GENERATE_REPORTS"] == "true" or
                                                 os.environ["GENERATE_REPORTS"] == "pre-release"):
            gd_file_name = "HUMAN" if data_provider == "Human" else data_provider
            release_version = ".".join(os.environ["RELEASE"].split(".")[0:2]) if "RELEASE" in os.environ else \
                "no-version"
            json_desc_writer.overall_properties.species = gd_file_name
            json_desc_writer.overall_properties.release_version = release_version
            json_desc_writer.overall_properties.date = datetime.date.today().strftime("%B %d, %Y")
            json_desc_writer.overall_properties.go_ontology_url = go_ontology_url
            json_desc_writer.overall_properties.go_association_url = go_association_url
            json_desc_writer.overall_properties.do_ontology_url = do_ontology_url
            json_desc_writer.overall_properties.do_association_url = do_association_url
            file_name = gd_file_name + "_gene_desc_" + datetime.date.today().strftime("%Y-%m-%d") + ".json"
            file_path = "tmp/" + file_name
            json_desc_writer.write_json(file_path=file_path, pretty=True, include_single_gene_stats=True)
            client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
                                  aws_secret_access_key=os.environ['AWS_SECRET_KEY'])
            if os.environ["GENERATE_REPORTS"] == "True" or os.environ["GENERATE_REPORTS"] == "true":
                client.upload_file(file_path, "agr-db-reports", "gene-descriptions/" + release_version + "/" +
                                   file_name)
            elif os.environ["GENERATE_REPORTS"] == "pre-release":
                client.upload_file(file_path, "agr-db-reports", "gene-descriptions/" + release_version +
                                   "/pre-release/" + file_name)
            # TODO create symlink to latest version
