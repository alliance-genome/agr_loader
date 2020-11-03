"""Gene Descriptions ETL."""

import copy
import logging
import os
import datetime
import re
import requests

from collections import defaultdict
from etl import ETL
from etl.helpers import Neo4jHelper
from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.descriptions_writer import DescriptionsWriter
from genedescriptions.gene_description import GeneDescription
from genedescriptions.data_manager import DataManager
from genedescriptions.commons import DataType, Gene
from genedescriptions.precanned_modules import set_gene_ontology_module, set_disease_module, \
                                               set_alliance_human_orthology_module,\
                                               set_expression_module
from ontobio import AssociationSetFactory, Ontology
from transactors import CSVTransactor, Neo4jTransactor
from loader_common import ContextInfo
from data_manager import DataFileManager
from generators.header import create_header

EXPRESSION_PRVD_SUBTYPE_MAP = {'WB': 'WBBT', 'ZFIN': 'ZFA', 'FB': 'FBBT', 'MGI': 'EMAPA'}


class GeneDescriptionsETL(ETL):
    """Gene Descriptions ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    gene_descriptions_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (o:Gene)
        WHERE o.primaryKey = row.genePrimaryKey
        SET o.automatedGeneSynopsis = row.geneDescription"""

    # Querys which do not take params and can be used as is

    get_all_genes_query = """
        MATCH (g:Gene)
        WHERE g.dataProvider = {parameter} AND NOT g.primaryKey CONTAINS "HGNC:"
        RETURN g.primaryKey, g.symbol"""

    get_all_genes_human_query = """
        MATCH (g:Gene)
        WHERE g.primaryKey CONTAINS "HGNC:"
        RETURN g.primaryKey, g.symbol"""

    get_gene_disease_annot_query = """
        MATCH (d:DOTerm:Ontology)-[r:IS_MARKER_FOR|IS_IMPLICATED_IN|IS_MODEL_OF]-(g:Gene)-[:ASSOCIATION]->
        (dga:Association:DiseaseEntityJoin)-[:ASSOCIATION]->(d)
        WHERE g.dataProvider = {parameter}
        MATCH (dga)-[:EVIDENCE]->(pec:PublicationJoin)-[:ASSOCIATION]-(e:ECOTerm)
        RETURN DISTINCT g.primaryKey AS geneId,
                        g.symbol AS geneSymbol,
                        d.primaryKey AS TermId,
                        e.primaryKey AS ECode,
                        type(r) AS relType,
                        'D' AS aspect"""

    get_feature_disease_annot_query = """
        MATCH (d:DOTerm:Ontology)-[r:IS_MARKER_FOR|IS_IMPLICATED_IN|IS_MODEL_OF]-(f)-[:ASSOCIATION]->
        (dga:Association:DiseaseEntityJoin)-[:ASSOCIATION]->(d)
        WHERE f.dataProvider = {parameter}
        MATCH (f)<-[:IS_ALLELE_OF]->(g:Gene)
        MATCH (dga)-[:EVIDENCE]->(pec:PublicationJoin)-[:ASSOCIATION]-(e:ECOTerm)
        RETURN DISTINCT g.primaryKey AS geneId,
                        g.symbol AS geneSymbol,
                        f.primaryKey as alleleId,
                        d.primaryKey as TermId,
                        e.primaryKey AS ECode,
                        type(r) AS relType,
                        'D' AS aspect"""

    get_filtered_human_orthologs_query = """
        MATCH (g2)<-[orth:ORTHOLOGOUS]-(g:Gene)-[:ASSOCIATION]->(ogj:Association:OrthologyGeneJoin)-[:ASSOCIATION]->
        (g2:Gene)
        WHERE ogj.joinType = 'orthologous' AND g.dataProvider = {parameter} AND g2.taxonId ='NCBITaxon:9606' AND
        orth.strictFilter = true
        MATCH (ogj)-[:MATCHED]->(oa:OrthoAlgorithm)
        RETURN g.primaryKey AS geneId,
               g2.primaryKey AS orthoId,
               g2.symbol AS orthoSymbol,
               g2.name AS orthoName,
               oa.name AS algorithm"""

    get_disease_via_orthology_query = """
        MATCH (d:DOTerm:Ontology)-[r:IMPLICATED_VIA_ORTHOLOGY]-(g:Gene)-[:ASSOCIATION]->
        (dga:Association:DiseaseEntityJoin)-[:ASSOCIATION]->(d)
        WHERE g.dataProvider = {parameter}
        MATCH (dga)-[:FROM_ORTHOLOGOUS_GENE]-(orthGene:Gene)
        WHERE orthGene.taxonId = 'NCBITaxon:9606'
        RETURN DISTINCT g.primaryKey AS geneId,
                        g.symbol AS geneSymbol,
                        d.primaryKey AS TermId"""

    get_ontology_pairs_query = """
        MATCH (term1:{}Term:Ontology)-[r:IS_A|PART_OF]->(term2:{}Term:Ontology)
        RETURN term1.primaryKey,
               term1.name,
               term1.type,
               term1.isObsolete,
               term2.primaryKey,
               term2.name,
               term2.type,
               term2.isObsolete,
               type(r) AS rel_type"""

    get_expression_annotations_query = """
        MATCH (g:Gene)-[EXPRESSED_IN]->(:ExpressionBioEntity)-[:ANATOMICAL_STRUCTURE|ANATOMICAL_SUB_STRUCTURE]->(t:Ontology)-[:IS_A|PART_OF]->(t2:Ontology)
        WHERE g.dataProvider = {parameter}
        RETURN g.primaryKey AS geneId,
               g.symbol AS geneSymbol,
               t.primaryKey AS TermId,
               'EXP' AS relType,
               'A' AS aspect"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config
        self.cur_date = datetime.date.today().strftime("%Y%m%d")

    def _load_and_process_data(self):
        # create gene descriptions data manager and load common data
        context_info = ContextInfo()
        data_manager = DataFileManager(context_info.config_file_location)
        # go_onto_config = data_manager.get_config('GO')
        go_annot_config = data_manager.get_config('GAF')
        # do_onto_config = data_manager.get_config('DOID')
        go_annot_sub_dict = {sub.get_data_provider(): sub for sub in go_annot_config.get_sub_type_objects()}
        this_dir = os.path.split(__file__)[0]
        gd_config = GenedescConfigParser(os.path.join(this_dir,
                                                      os.pardir,
                                                      os.pardir,
                                                      "gene_descriptions.yml"))
        gd_data_manager = DataManager(do_relations=None, go_relations=["subClassOf", "BFO:0000050"])
        gd_data_manager.set_ontology(ontology_type=DataType.GO,
                                     ontology=self.get_ontology(data_type=DataType.GO),
                                     config=gd_config)
        gd_data_manager.set_ontology(ontology_type=DataType.DO,
                                     ontology=self.get_ontology(data_type=DataType.DO),
                                     config=gd_config)
        # generate descriptions for each MOD
        for prvdr in [sub_type.get_data_provider().upper()
                      for sub_type in self.data_type_config.get_sub_type_objects()]:
            gd_config_mod_specific = copy.deepcopy(gd_config)
            if prvdr == "WB":
                gd_config_mod_specific.config["expression_sentences_options"][
                    "remove_children_if_parent_is_present"] = True
            self.logger.info("Generating gene descriptions for %s", prvdr)
            data_provider = prvdr if prvdr != "HUMAN" else "RGD"
            json_desc_writer = DescriptionsWriter()
            go_annot_path = "file://" + os.path.join(os.getcwd(),
                                                     "tmp",
                                                     go_annot_sub_dict[prvdr].get_filepath())
            gd_data_manager.load_associations_from_file(
                associations_type=DataType.GO, associations_url=go_annot_path,
                associations_cache_path=os.path.join(os.getcwd(),
                                                     "tmp",
                                                     "gd_cache",
                                                     "go_annot_" + prvdr + ".gaf"),
                config=gd_config_mod_specific)
            gd_data_manager.set_associations(associations_type=DataType.DO,
                                             associations=self.get_disease_annotations_from_db(
                                                 data_provider=data_provider,
                                                 gd_data_manager=gd_data_manager,
                                                 logger=self.logger),
                                             config=gd_config_mod_specific)
            if prvdr in EXPRESSION_PRVD_SUBTYPE_MAP:
                gd_data_manager.set_ontology(ontology_type=DataType.EXPR,
                                             ontology=self.get_ontology(data_type=DataType.EXPR,
                                                                        provider=prvdr),
                                             config=gd_config_mod_specific)
                gd_data_manager.set_associations(
                    associations_type=DataType.EXPR,
                    associations=self.get_expression_annotations_from_db(data_provider=data_provider,
                                                                         gd_data_manager=gd_data_manager,
                                                                         logger=self.logger),
                    config=gd_config_mod_specific)
            commit_size = self.data_type_config.get_neo4j_commit_size()
            generators = self.get_generators(prvdr,
                                             gd_data_manager,
                                             gd_config_mod_specific,
                                             json_desc_writer)
            query_template_list = [
                [self.gene_descriptions_query_template, commit_size,
                 "genedescriptions_data_" + prvdr + ".csv"]
            ]

            query_and_file_list = self.process_query_params(query_template_list)
            CSVTransactor.save_file_static(generators, query_and_file_list)
            Neo4jTransactor.execute_query_batch(query_and_file_list)
            self.error_messages()

            self.save_descriptions_report_files(data_provider=prvdr,
                                                json_desc_writer=json_desc_writer,
                                                context_info=context_info,
                                                gd_data_manager=gd_data_manager)

    def get_generators(self, data_provider, gd_data_manager, gd_config, json_desc_writer):
        """Create generators."""
        gene_prefix = ""
        if data_provider == "HUMAN":
            return_set = Neo4jHelper.run_single_query(self.get_all_genes_human_query)
            gene_prefix = "RGD:"
        else:
            return_set = Neo4jHelper.run_single_parameter_query(self.get_all_genes_query,
                                                                data_provider)
        descriptions = []
        best_orthologs = self.get_best_orthologs_from_db(data_provider=data_provider)
        for record in return_set:
            gene = Gene(id=gene_prefix + record["g.primaryKey"],
                        name=record["g.symbol"],
                        dead=False,
                        pseudo=False)
            gene_desc = GeneDescription(gene_id=record["g.primaryKey"],
                                        gene_name=gene.name,
                                        add_gene_name=False,
                                        config=gd_config)
            set_gene_ontology_module(dm=gd_data_manager,
                                     conf_parser=gd_config,
                                     gene_desc=gene_desc, gene=gene)
            set_expression_module(df=gd_data_manager,
                                  conf_parser=gd_config,
                                  gene_desc=gene_desc,
                                  gene=gene)
            set_disease_module(df=gd_data_manager,
                               conf_parser=gd_config,
                               gene_desc=gene_desc,
                               gene=gene,
                               human=data_provider == "HUMAN")
            if gene.id in best_orthologs:
                gene_desc.stats.set_best_orthologs = best_orthologs[gene.id][0]
                set_alliance_human_orthology_module(orthologs=best_orthologs[gene.id][0],
                                                    excluded_orthologs=best_orthologs[gene.id][1],
                                                    gene_desc=gene_desc,
                                                    config=gd_config)

            if gene_desc.description:
                descriptions.append({
                    "genePrimaryKey": gene_desc.gene_id,
                    "geneDescription": gene_desc.description
                })
            json_desc_writer.add_gene_desc(gene_desc)
        yield [descriptions]

    def get_ontology(self, data_type: DataType, provider=None):
        """Get Ontology."""
        ontology = Ontology()
        terms_pairs = []
        if data_type == DataType.GO:
            terms_pairs = Neo4jHelper.run_single_parameter_query(
                self.get_ontology_pairs_query.format("GO", "GO"),
                None)
        elif data_type == DataType.DO:
            terms_pairs = Neo4jHelper.run_single_parameter_query(
                self.get_ontology_pairs_query.format("DO", "DO"),
                None)
        elif data_type == DataType.EXPR:
            if provider in EXPRESSION_PRVD_SUBTYPE_MAP:
                terms_pairs = Neo4jHelper.run_single_parameter_query(
                    self.get_ontology_pairs_query.format(EXPRESSION_PRVD_SUBTYPE_MAP[provider],
                                                         EXPRESSION_PRVD_SUBTYPE_MAP[provider]),
                    None)
        for terms_pair in terms_pairs:
            self.add_neo_term_to_ontobio_ontology_if_not_exists(
                terms_pair["term1.primaryKey"], terms_pair["term1.name"], terms_pair["term1.type"],
                terms_pair["term1.isObsolete"], ontology)
            self.add_neo_term_to_ontobio_ontology_if_not_exists(
                terms_pair["term2.primaryKey"], terms_pair["term2.name"], terms_pair["term2.type"],
                terms_pair["term2.isObsolete"], ontology)
            ontology.add_parent(terms_pair["term1.primaryKey"], terms_pair["term2.primaryKey"],
                                relation="subClassOf" if terms_pair["rel_type"] == "IS_A" else "BFO:0000050")
        if data_type == DataType.EXPR and provider == "MGI":
            self.add_neo_term_to_ontobio_ontology_if_not_exists("EMAPA_ARTIFICIAL_NODE:99999",
                                                                "embryo",
                                                                "anatomical_structure",
                                                                False,
                                                                ontology)
            ontology.add_parent("EMAPA_ARTIFICIAL_NODE:99999", "EMAPA:0", relation="subClassOf")
            self.add_neo_term_to_ontobio_ontology_if_not_exists("EMAPA_ARTIFICIAL_NODE:99998",
                                                                "head",
                                                                "anatomical_structure",
                                                                False,
                                                                ontology)
            ontology.add_parent("EMAPA_ARTIFICIAL_NODE:99998", "EMAPA:0", relation="subClassOf")
            GeneDescriptionsETL.add_neo_term_to_ontobio_ontology_if_not_exists(
                "EMAPA_ARTIFICIAL_NODE:99997",
                "gland",
                "anatomical_structure",
                False,
                ontology)
            ontology.add_parent("EMAPA_ARTIFICIAL_NODE:99997", "EMAPA:0", relation="subClassOf")
        elif data_type == DataType.EXPR and provider == "FB":
            GeneDescriptionsETL.add_neo_term_to_ontobio_ontology_if_not_exists(
                "FBbt_ARTIFICIAL_NODE:99999",
                "organism",
                "",
                False,
                ontology)
            ontology.add_parent("FBbt_ARTIFICIAL_NODE:99999",
                                "FBbt:10000000",
                                relation="subClassOf")

        return ontology

    @staticmethod
    def add_neo_term_to_ontobio_ontology_if_not_exists(term_id, term_label,
                                                       term_type, is_obsolete, ontology):
        """Add NEO Term to Ontobio Ontology If Not Exists."""
        if not ontology.has_node(term_id) and term_label:
            if is_obsolete in ["true", "True"]:
                meta = {"deprecated": True, "basicPropertyValues": [
                    {"pred": "OIO:hasOBONamespace", "val": term_type}]}
            else:
                meta = {"basicPropertyValues": [
                    {"pred": "OIO:hasOBONamespace", "val": term_type}]}
            ontology.add_node(id=term_id, label=term_label, meta=meta)

    @staticmethod
    def create_annotation_record(gene_id, gene_symbol, term_id, aspect, ecode, prvdr, qualifier):
        """Create Annotation Record."""
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
                    "id": term_id,
                    "taxon": ""
                },
                "qualifiers": [qualifier],
                "aspect": aspect,
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
    def add_annotations(final_annotation_set, neo4j_annot_set, data_provider,
                        data_type: DataType, logger, ontology=None):
        """Add Annotations."""
        early_conceptus_re = re.compile(r'.*stage conceptus$')
        qualifier = ""
        for annot in neo4j_annot_set:
            if data_type == DataType.DO:
                ecodes = ["EXP"] if annot["relType"] != "IS_MARKER_FOR" else ["BMK"]
            elif data_type == DataType.EXPR:
                ecodes = ["EXP"]
                qualifier = "Verified"
                # map direct annotations to 'embryo' in mouse or 'organism'
                # in fly to a new 'artificial' node to keep
                # them but avoid 'embryo' and 'organism' as common ancestors at the same time
                if annot["TermId"] == "EMAPA:16039":
                    annot = {key: value for key, value in annot.items()}
                    annot["TermId"] = "EMAPA_ARTIFICIAL_NODE:99999"
                elif annot["TermId"] == "EMAPA:31858":
                    annot = {key: value for key, value in annot.items()}
                    annot["TermId"] = "EMAPA_ARTIFICIAL_NODE:99998"
                elif annot["TermId"] == "EMAPA:18425":
                    annot = {key: value for key, value in annot.items()}
                    annot["TermId"] = "EMAPA_ARTIFICIAL_NODE:99997"
                elif annot["TermId"] == "FBbt:00000001":
                    annot = {key: value for key, value in annot.items()}
                    annot["TermId"] = "FBbt_ARTIFICIAL_NODE:99999"
                # map all annotations to '* stage conceptus' to 'early conceptus'
                if early_conceptus_re.match(ontology.label(annot["TermId"])):
                    annot = {key: value for key, value in annot.items()}
                    annot["TermId"] = "EMAPA:36473"
            else:
                ecodes = [annot["ECode"]]
            for ecode in ecodes:
                logger.debug(ecode)
                final_annotation_set.append(GeneDescriptionsETL.create_annotation_record(
                    annot["geneId"]
                    if not annot["geneId"].startswith("HGNC:") else "RGD:" + annot["geneId"],
                    annot["geneSymbol"],
                    annot["TermId"],
                    annot["aspect"],
                    ecode,
                    data_provider,
                    qualifier))

    @staticmethod
    def get_disease_annotations_from_db(data_provider, gd_data_manager, logger):
        """Get Disease Annotations From DB."""
        annotations = []
        gene_annot_set = Neo4jHelper.run_single_parameter_query(
            GeneDescriptionsETL.get_gene_disease_annot_query,
            data_provider)
        GeneDescriptionsETL.add_annotations(annotations,
                                            gene_annot_set,
                                            data_provider,
                                            DataType.DO,
                                            logger)

        feature_annot_set = Neo4jHelper.run_single_parameter_query(
            GeneDescriptionsETL.get_feature_disease_annot_query,
            data_provider)
        allele_do_annot = defaultdict(list)
        for feature_annot in feature_annot_set:
            if all([feature_annot["geneId"] != annot[0]
                    for annot in allele_do_annot[(feature_annot["alleleId"],
                                                  feature_annot["TermId"])]]):
                allele_do_annot[(feature_annot["alleleId"],
                                 feature_annot["TermId"])].append(feature_annot)
        # keep only disease annotations through simple entities
        # (e.g., alleles related to one gene only)
        feature_annot_set = [feature_annots[0] for feature_annots in allele_do_annot.values() if
                             len(feature_annots) == 1]
        GeneDescriptionsETL.add_annotations(annotations,
                                            feature_annot_set,
                                            data_provider,
                                            DataType.DO,
                                            logger)
        disease_via_orth_records = Neo4jHelper.run_single_parameter_query(
            GeneDescriptionsETL.get_disease_via_orthology_query, data_provider)
        for orth_annot in disease_via_orth_records:
            annotations.append(GeneDescriptionsETL.create_annotation_record(
                gene_id=orth_annot["geneId"],
                gene_symbol=orth_annot["geneSymbol"],
                term_id=orth_annot["TermId"],
                aspect="D",
                ecode="DVO",
                prvdr=data_provider,
                qualifier=""))
        return AssociationSetFactory().create_from_assocs(assocs=list(annotations),
                                                          ontology=gd_data_manager.do_ontology)

    @staticmethod
    def get_expression_annotations_from_db(data_provider, gd_data_manager, logger):
        """Get Expression Annotations From DB."""
        annotations = []
        gene_annot_set = Neo4jHelper.run_single_parameter_query(
            GeneDescriptionsETL.get_expression_annotations_query,
            data_provider)
        GeneDescriptionsETL.add_annotations(annotations,
                                            gene_annot_set,
                                            data_provider,
                                            DataType.EXPR,
                                            logger,
                                            gd_data_manager.expression_ontology)
        return AssociationSetFactory().create_from_assocs(
            assocs=list(annotations),
            ontology=gd_data_manager.expression_ontology)

    @staticmethod
    def get_best_orthologs_from_db(data_provider):
        """Get Best Orthologs_from_db."""
        orthologs_set = Neo4jHelper.run_single_parameter_query(
            GeneDescriptionsETL.get_filtered_human_orthologs_query,
            data_provider)
        genes_orthologs_algos = defaultdict(lambda: defaultdict(int))
        best_orthologs = {}
        orthologs_info = {}
        for ortholog_algo in orthologs_set:
            genes_orthologs_algos[ortholog_algo["geneId"]][ortholog_algo["orthoId"]] += 1
            if ortholog_algo["orthoId"] not in orthologs_info:
                orthologs_info[ortholog_algo["orthoId"]] = (ortholog_algo["orthoSymbol"],
                                                            ortholog_algo["orthoName"])
        for gene_id in genes_orthologs_algos.keys():
            best_orthologs[gene_id] = [[[ortholog_id,
                                         orthologs_info[ortholog_id][0],
                                         orthologs_info[ortholog_id][1]]
                                        for ortholog_id in genes_orthologs_algos[gene_id].keys() if
                                        genes_orthologs_algos[gene_id][ortholog_id] ==
                                        max(genes_orthologs_algos[gene_id].values())], False]
            best_orthologs[gene_id][-1] \
                = len(best_orthologs[gene_id][0]) != len(genes_orthologs_algos[gene_id].keys())
        return best_orthologs

    @staticmethod
    def upload_files_to_fms(file_path, context_info, data_provider, logger):
        """Upload Files To FMS."""
        with open(file_path + ".json", 'rb') as f_json, \
            open(file_path + ".txt", 'rb') as f_txt, \
                open(file_path + ".tsv", 'rb') as f_tsv:
            if context_info.env["GENERATE_REPORTS"] is True:
                file_to_upload = {
                    f"{context_info.env['ALLIANCE_RELEASE']}_GENE-DESCRIPTION-JSON_{data_provider}": f_json,
                    f"{context_info.env['ALLIANCE_RELEASE']}_GENE-DESCRIPTION-TXT_{data_provider}": f_txt,
                    f"{context_info.env['ALLIANCE_RELEASE']}_GENE-DESCRIPTION-TSV_{data_provider}": f_tsv}
            else:
                file_to_upload = {
                    f"{context_info.env['ALLIANCE_RELEASE']}_GENE-DESCRIPTION-TEST-JSON_{data_provider}": f_json}

            headers = {
                'Authorization': 'Bearer {}'.format(context_info.env['API_KEY'])
            }

            logger.debug(file_to_upload)
            logger.debug(headers)
            logger.debug('Uploading gene description files to FMS %s',
                         context_info.env['FMS_API_URL'])
            response = requests.post(context_info.env['FMS_API_URL'] + '/api/data/submit',
                                     files=file_to_upload,
                                     headers=headers)
            logger.info(response.text)

    def save_descriptions_report_files(self, data_provider, json_desc_writer, context_info, gd_data_manager):
        """Save Descripitons Report Files."""
        release_version = ".".join(context_info.env["ALLIANCE_RELEASE"].split(".")[0:2])
        json_desc_writer.overall_properties.species = data_provider
        json_desc_writer.overall_properties.release_version = release_version
        json_desc_writer.overall_properties.date = self.cur_date
        file_name = self.cur_date + "_" + data_provider
        file_path = os.path.join("tmp", file_name)
        json_desc_writer.write_json(file_path=file_path + ".json",
                                    pretty=True,
                                    include_single_gene_stats=True,
                                    data_manager=gd_data_manager)
        json_desc_writer.write_plain_text(file_path=file_path + ".txt")
        readme = "This file contains the following fields: gene ID, gene name, and gene description. The gene " \
                 "descriptions are generated by an algorithm developed by the Alliance that uses highly structured " \
                 "gene data such as associations to various ontology terms (e.g., Gene Ontology terms) and the " \
                 "Alliance strict orthology set. The original set of ontology terms that a gene is annotated to may " \
                 "have been trimmed to an ancestor term in the ontology, in order to balance readability with the " \
                 "amount of information in the description. The complete set of annotations to any gene in this file " \
                 "may be found in the relevant data tables on the Alliance gene page."
        species = self.etlh.species_lookup_by_data_provider(data_provider)
        taxon_id = self.etlh.get_taxon_from_mod(data_provider)
        header = create_header(file_type='Gene Descriptions', database_version=context_info.env["ALLIANCE_RELEASE"],
                               data_format='txt', readme=readme, species=species, taxon_ids='# TaxonIDs:NCBITaxon:' +
                                                                                            taxon_id)
        header = "\n".join([line.strip() for line in header.splitlines() if len(line.strip()) != 0])
        self.add_header_to_file(file_path=file_path + ".txt", header=header)
        json_desc_writer.write_tsv(file_path=file_path + ".tsv")
        header = create_header(file_type='Gene Descriptions', database_version=context_info.env["ALLIANCE_RELEASE"],
                               data_format='tsv', readme=readme, species=species, taxon_ids='# TaxonIDs:NCBITaxon:' +
                                                                                            taxon_id)
        header = "\n".join([line.strip() for line in header.splitlines() if len(line.strip()) != 0])
        self.add_header_to_file(file_path=file_path + ".tsv", header=header)
        if context_info.env["GENERATE_REPORTS"]:
            self.upload_files_to_fms(file_path, context_info, data_provider, self.logger)

    @staticmethod
    def add_header_to_file(file_path, header):
        """Add header to file."""
        with open(file_path, 'r') as original:
            data = original.read()
        with open(file_path, 'w') as modified:
            modified.write(header + "\n\n" + data)
