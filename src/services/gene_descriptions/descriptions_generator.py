import datetime
from typing import Dict

from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.data_fetcher import DataFetcher, Gene, DataType
from genedescriptions.descriptions_rules import GeneDesc, SentenceGenerator
from genedescriptions.descriptions_writer import JsonGDWriter
from ontobio import AssociationSetFactory
from services.gene_descriptions.descriptions_writer import Neo4jGDWriter
from ontobio.ontol import Ontology


class GeneDescGenerator(object):

    def __init__(self, config_file_path: str, go_ontology, do_ontology, graph_db):
        self.conf_parser = GenedescConfigParser(config_file_path)
        self.graph = graph_db
        self.go_ontology = go_ontology
        self.do_ontology = do_ontology
        self.go_sent_gen_common_props = {
            "evidence_groups_priority_list": self.conf_parser.get_go_evidence_groups_priority_list(),
            "prepostfix_sentences_map": self.conf_parser.get_go_prepostfix_sentences_map(),
            "prepostfix_special_cases_sent_map": self.conf_parser.get_go_prepostfix_special_cases_sent_map(),
            "evidence_codes_groups_map": self.conf_parser.get_go_evidence_codes_groups_map()}
        self.go_sent_common_props = {
            "remove_parent_terms": True,
            "merge_num_terms_threshold": 3,
            "merge_min_distance_from_root": self.conf_parser.get_go_trim_min_distance_from_root(),
            "truncate_others_generic_word": self.conf_parser.get_go_truncate_others_aggregation_word(),
            "truncate_others_aspect_words": self.conf_parser.get_go_truncate_others_terms(),
            "add_multiple_if_covers_more_children": False}
        self.do_sent_gen_common_prop = {
            "evidence_groups_priority_list": self.conf_parser.get_do_evidence_groups_priority_list(),
            "prepostfix_sentences_map": self.conf_parser.get_do_prepostfix_sentences_map(),
            "prepostfix_special_cases_sent_map": None,
            "evidence_codes_groups_map": self.conf_parser.get_do_evidence_codes_groups_map()}
        self.do_sent_common_props = {
            "remove_parent_terms": True,
            "merge_num_terms_threshold": 3,
            "merge_min_distance_from_root": self.conf_parser.get_do_trim_min_distance_from_root(),
            "truncate_others_generic_word": self.conf_parser.get_do_truncate_others_aggregation_word(),
            "truncate_others_aspect_words": self.conf_parser.get_do_truncate_others_terms(),
            "add_multiple_if_covers_more_children": True}

    @staticmethod
    def get_ontology_from_loader_object(ontology_term_list) -> Ontology:
        ontology = Ontology()
        for term in ontology_term_list:
            if ontology.has_node(term["oid"]):
                # previously added as parent
                ontology.node(term["oid"])["label"] = term["name"]
            else:
                ontology.add_node(term["oid"], term["name"])
            if term["is_obsolete"] == "true":
                ontology.set_obsolete(term["oid"])
            ontology.node(term["oid"])["alt_ids"] = term["alt_ids"]
            for parent_id in term["isas"]:
                if not ontology.has_node(parent_id):
                    ontology.add_node(parent_id, "")
                ontology.add_parent(term["oid"], parent_id, "subClassOf")
            for parent_id in term["partofs"]:
                if not ontology.has_node(parent_id):
                    ontology.add_node(parent_id, "")
                ontology.add_parent(term["oid"], parent_id, "BFO:0000050")
        return ontology

    def get_go_associations_from_loader_object(self, go_annotations):
        associations = []
        for gene_annotations in go_annotations:
            for annot in gene_annotations["annotations"]:
                if self.go_ontology.has_node(annot["go_id"]) and not self.go_ontology.is_obsolete(annot["go_id"]):
                    associations.append({"source_line": "",
                                         "subject": {
                                             "id": gene_annotations["gene_id"],
                                             "label": "",
                                             "type": "",
                                             "fullname": "",
                                             "synonyms": [],
                                             "taxon": {"id": ""}

                                         },
                                         "object": {
                                             "id": annot["go_id"],
                                             "taxon": ""
                                         },
                                         "qualifiers": annot["qualifier"].split("|"),
                                         "aspect": annot["aspect"],
                                         "relation": {"id": None},
                                         "negated": False,
                                         "evidence": {
                                             "type": annot["evidence_code"],
                                             "has_supporting_reference": "",
                                             "with_support_from": [],
                                             "provided_by": gene_annotations["dataProvider"],
                                             "date": None
                                             }
                                         })
        return AssociationSetFactory().create_from_assocs(assocs=associations, ontology=self.go_ontology)

    def get_do_associations_from_loader_object(self, do_annotations, do_annotations_allele):
        associations = []
        for gene_annotations in do_annotations:
            for annot in gene_annotations:
                if annot and "doId" in annot and self.do_ontology.has_node(annot["doId"]) and not \
                        self.do_ontology.is_obsolete(annot["doId"]):
                    associations.append({"source_line": "",
                                         "subject": {
                                             "id": annot["primaryId"],
                                             "label": annot["diseaseObjectName"],
                                             "type": annot["diseaseObjectType"],
                                             "fullname": "",
                                             "synonyms": [],
                                             "taxon": {"id": ""}

                                         },
                                         "object": {
                                             "id": annot["doId"],
                                             "taxon": ""
                                         },
                                         "qualifiers":
                                             annot["qualifier"].split("|") if annot["qualifier"] is not None else "",
                                         "aspect": "D",
                                         "relation": {"id": None},
                                         "negated": False,
                                         "evidence": {
                                             "type": annot["ecodes"][0],
                                             "has_supporting_reference": "",
                                             "with_support_from": [],
                                             "provided_by": annot["dataProvider"],
                                             "date": None
                                             }
                                         })
        for gene_annotations in do_annotations_allele:
            for annot in gene_annotations:
                inferred_genes = self.get_inferred_genes_for_allele(annot["primaryId"])
                if len(inferred_genes) == 1 and annot and "doId" in annot and self.do_ontology.has_node(annot["doId"]) \
                        and not self.do_ontology.is_obsolete(annot["doId"]):
                    associations.append({"source_line": "",
                                         "subject": {
                                             "id": inferred_genes[0].id,
                                             "label": annot["diseaseObjectName"],
                                             "type": annot["diseaseObjectType"],
                                             "fullname": "",
                                             "synonyms": [],
                                             "taxon": {"id": ""}

                                         },
                                         "object": {
                                             "id": annot["doId"],
                                             "taxon": ""
                                         },
                                         "qualifiers":
                                             annot["qualifier"].split("|") if annot["qualifier"] is not None else "",
                                         "aspect": "D",
                                         "relation": {"id": None},
                                         "negated": False,
                                         "evidence": {
                                             "type": annot["ecodes"][0],
                                             "has_supporting_reference": "",
                                             "with_support_from": [],
                                             "provided_by": annot["dataProvider"],
                                             "date": None
                                         }
                                         })
        return AssociationSetFactory().create_from_assocs(assocs=associations, ontology=self.do_ontology)

    @staticmethod
    def query_db(db_graph, query: str, parameters: Dict = None):
        with db_graph.session() as session:
            with session.begin_transaction() as tx:
                return_set = tx.run(query, parameters=parameters)
        return return_set

    def get_gene_data_from_neo4j(self, data_provider):
        db_query = "match (g:Gene) where g.dataProvider = {dataProvider} return g.symbol, g.primaryKey"
        result_set = self.query_db(db_graph=self.graph, query=db_query,
                                   parameters={"dataProvider": data_provider})
        for result in result_set:
            yield Gene(result["g.primaryKey"], result["g.symbol"], False, False)

    def get_inferred_genes_for_allele(self, allele_primary_key):
        db_query = "match (o:Feature)-[:IS_ALLELE_OF]-(g:Gene) where o.primaryKey = {allelePrimaryKey} return " \
                   "g.symbol, g.primaryKey"
        result_set = self.query_db(db_graph=self.graph, query=db_query,
                                   parameters={"allelePrimaryKey": allele_primary_key})
        return [Gene(result["g.primaryKey"], result["g.symbol"], False, False) for result in result_set]

    def generate_descriptions(self, go_annotations, do_annotations, do_annotations_allele, data_provider,
                              cached_data_fetcher, human=False) -> DataFetcher:
        # Generate gene descriptions and save to db
        desc_writer = Neo4jGDWriter()
        json_desc_writer = JsonGDWriter()
        if cached_data_fetcher:
            df = cached_data_fetcher
        else:
            df = DataFetcher(go_relations=["subClassOf", "BFO:0000050"], do_relations=None)
            df.set_ontology(ontology_type=DataType.GO, ontology=self.go_ontology,
                            terms_replacement_regex=self.conf_parser.get_go_rename_terms())
            df.set_ontology(ontology_type=DataType.DO, ontology=self.do_ontology, terms_replacement_regex=None)
        df.set_associations(associations_type=DataType.GO,
                            associations=self.get_go_associations_from_loader_object(go_annotations),
                            exclusion_list=self.conf_parser.get_go_terms_exclusion_list())
        df.set_associations(associations_type=DataType.DO,
                            associations=self.get_do_associations_from_loader_object(do_annotations,
                                                                                     do_annotations_allele),
                            exclusion_list=self.conf_parser.get_do_terms_exclusion_list())
        for gene in self.get_gene_data_from_neo4j(data_provider=data_provider):
            gene_desc = GeneDesc(gene_id=gene.id, gene_name=gene.name)
            joined_sent = []
            go_sent_generator = SentenceGenerator(
                annotations=df.get_annotations_for_gene(gene_id=gene.id, annot_type=DataType.GO,
                                                        priority_list=self.conf_parser.get_go_annotations_priority()),
                ontology=df.go_ontology, **self.go_sent_gen_common_props)

            func_sent = " and ".join([sentence.text for sentence in go_sent_generator.get_sentences(
                aspect='F', merge_groups_with_same_prefix=True, keep_only_best_group=True, )])
            if func_sent:
                joined_sent.append(func_sent)
            contributes_to_func_sent = " and ".join([sentence.text for sentence in go_sent_generator.get_sentences(
                aspect='F', qualifier='contributes_to', merge_groups_with_same_prefix=True,
                keep_only_best_group=True, **self.go_sent_common_props)])
            if contributes_to_func_sent:
                joined_sent.append(contributes_to_func_sent)
            proc_sent = " and ".join([sentence.text for sentence in go_sent_generator.get_sentences(
                aspect='P', merge_groups_with_same_prefix=True, keep_only_best_group=True,
                **self.go_sent_common_props)])
            if proc_sent:
                joined_sent.append(proc_sent)
            comp_sent = " and ".join([sentence.text for sentence in go_sent_generator.get_sentences(
                aspect='C', merge_groups_with_same_prefix=True, keep_only_best_group=True,
                **self.go_sent_common_props)])
            if comp_sent:
                joined_sent.append(comp_sent)
            colocalizes_with_comp_sent = " and ".join([sentence.text for sentence in go_sent_generator.get_sentences(
                aspect='C', qualifier='colocalizes_with', merge_groups_with_same_prefix=True,
                keep_only_best_group=True, **self.go_sent_common_props)])
            if colocalizes_with_comp_sent:
                joined_sent.append(colocalizes_with_comp_sent)

            if len(joined_sent) > 0:
                desc = "; ".join(joined_sent) + "."
                if len(desc) > 0:
                    gene_desc.go_description = desc[0].upper() + desc[1:]
                else:
                    gene_desc.go_description = None
            else:
                gene_desc.go_description = None

            prepostfix_sent_map = self.conf_parser.get_do_prepostfix_sentences_map()
            if human:
                prepostfix_sent_map = self.conf_parser.get_do_prepostfix_sentences_map_humans()
            do_sentence_generator = SentenceGenerator(
                df.get_annotations_for_gene(gene_id=gene.id, annot_type=DataType.DO,
                                            priority_list=self.conf_parser.get_do_annotations_priority()),
                ontology=df.do_ontology,
                evidence_groups_priority_list=self.conf_parser.get_do_evidence_groups_priority_list(),
                prepostfix_sentences_map=prepostfix_sent_map,
                prepostfix_special_cases_sent_map=None,
                evidence_codes_groups_map=self.conf_parser.get_do_evidence_codes_groups_map())
            disease_sent = "; ".join([sentence.text for sentence in do_sentence_generator.get_sentences(
                aspect='D', merge_groups_with_same_prefix=True, keep_only_best_group=False,
                **self.do_sent_common_props)])
            if disease_sent and len(disease_sent) > 0:
                gene_desc.do_description = disease_sent[0].upper() + disease_sent[1:]
                joined_sent.append(disease_sent)
            else:
                gene_desc.do_description = None

            if len(joined_sent) > 0:
                desc = "; ".join(joined_sent) + "."
                if len(desc) > 0:
                    gene_desc.description = desc[0].upper() + desc[1:]
                else:
                    gene_desc.description = None
            else:
                gene_desc.description = None
            desc_writer.add_gene_desc(gene_desc)
            json_desc_writer.add_gene_desc(gene_desc)

        desc_writer.write(self.graph)
        gd_file_name = data_provider
        if human:
            gd_file_name = "HUMAN"
        json_desc_writer.overall_properties.species = gd_file_name
        json_desc_writer.overall_properties.release_version = "1.6"
        json_desc_writer.overall_properties.date = datetime.date.today().strftime("%B %d, %Y")
        json_desc_writer.write("tmp/" + gd_file_name + "_with_stats.json", pretty=True, include_single_gene_stats=True)
        json_desc_writer.write("tmp/" + gd_file_name + "_no_stats.json", pretty=True, include_single_gene_stats=False)
        return df
