from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.data_fetcher import AnnotationType
from genedescriptions.descriptions_rules import GeneDesc, generate_sentences
from services.gene_descriptions.data_fetcher import AGRLoaderDataFetcher
from services.gene_descriptions.descriptions_writer import Neo4jGDWriter


class GeneDescGenerator(object):

    def __init__(self, config_file_path: str, go_dataset, do_dataset, graph_db):
        self.conf_parser = GenedescConfigParser(config_file_path)
        self.exclusion_list = self.conf_parser.get_go_terms_exclusion_list()
        self.go_prepostfix_sentences_map = self.conf_parser.get_go_prepostfix_sentences_map()
        self.go_prepostfix_special_cases_sent_map = self.conf_parser.get_go_prepostfix_special_cases_sent_map()
        self.go_annotations_priority = self.conf_parser.get_go_annotations_priority()
        self.evidence_codes_groups_map = self.conf_parser.get_go_evidence_codes_groups_map()
        self.evidence_groups_priority_list = self.conf_parser.get_go_evidence_groups_priority_list()
        self.go_terms_replacement_dict = self.conf_parser.get_go_rename_terms()
        self.go_truncate_others_aggregation_word = self.conf_parser.get_go_truncate_others_aggregation_word()
        self.go_truncate_others_terms = self.conf_parser.get_go_truncate_others_terms()
        self.go_trim_min_distance_from_root = self.conf_parser.get_go_trim_min_distance_from_root()
        self.do_annotations_priority = self.conf_parser.get_do_annotations_priority()
        self.do_evidence_groups_priority_list = self.conf_parser.get_do_evidence_groups_priority_list()
        self.do_prepostfix_sentences_map = self.conf_parser.get_do_prepostfix_sentences_map()
        self.do_evidence_codes_groups_map = self.conf_parser.get_do_evidence_codes_groups_map()
        self.do_trim_min_distance_from_root = self.conf_parser.get_do_trim_min_distance_from_root()
        self.do_truncate_others_aggregation_word = self.conf_parser.get_do_truncate_others_aggregation_word()
        self.do_truncate_others_terms = self.conf_parser.get_do_truncate_others_terms()
        self.cached_go_ontology = None
        self.cached_do_ontology = None
        self.graph = graph_db
        self.go_dataset = go_dataset
        self.do_dataset = do_dataset

    def generate_descriptions(self, go_annotations, do_annotations, data_provider):
        # Generate gene descriptions and save to db
        desc_writer = Neo4jGDWriter()

        df = AGRLoaderDataFetcher(go_terms_exclusion_list=self.exclusion_list,
                                  go_terms_replacement_dict=self.go_terms_replacement_dict,
                                  db_graph=self.graph, go_ontology=self.cached_go_ontology,
                                  do_ontology=self.cached_do_ontology, data_provider=data_provider)
        df.load_go_data(go_terms_list=[self.go_dataset.node(i) for i in self.go_dataset.nodes()], go_annotations=go_annotations)
        # df.load_disease_data(do_terms_list=self.do_dataset, disease_annotations=do_annotations)
        # load go ontology only for the first data provider, use cached data for the others
        if not self.cached_go_ontology:
            self.cached_go_ontology = df.get_go_ontology()
        for gene in df.get_gene_data():
            gene_desc = GeneDesc(gene_id=gene.id, gene_name=gene.name)
            joined_sent = []
            go_sentences = generate_sentences(df.get_annotations(
                geneid=gene.id, annot_type=AnnotationType.GO,
                priority_list=self.go_annotations_priority, desc_stats=gene_desc.stats),
                ontology=df.get_go_ontology(),
                evidence_groups_priority_list=self.evidence_groups_priority_list,
                prepostfix_sentences_map=self.go_prepostfix_sentences_map,
                prepostfix_special_cases_sent_map=self.go_prepostfix_special_cases_sent_map,
                evidence_codes_groups_map=self.evidence_codes_groups_map,
                remove_parent_terms=True,
                merge_num_terms_threshold=3,
                merge_min_distance_from_root=self.go_trim_min_distance_from_root,
                desc_stats=gene_desc.stats, terms_replacement_dict=self.go_terms_replacement_dict,
                truncate_others_generic_word=self.go_truncate_others_aggregation_word,
                truncate_others_aspect_words=self.go_truncate_others_terms)
            if go_sentences:
                func_sent = " and ".join([sentence.text for sentence in go_sentences.get_sentences(
                    aspect='F', merge_groups_with_same_prefix=True, keep_only_best_group=True,
                    desc_stats=gene_desc.stats)])
                if func_sent:
                    joined_sent.append(func_sent)
                contributes_to_func_sent = " and ".join([sentence.text for sentence in go_sentences.get_sentences(
                    aspect='F', qualifier='contributes_to', merge_groups_with_same_prefix=True,
                    keep_only_best_group=True, desc_stats=gene_desc.stats)])
                if contributes_to_func_sent:
                    joined_sent.append(contributes_to_func_sent)
                proc_sent = " and ".join([sentence.text for sentence in go_sentences.get_sentences(
                    aspect='P', merge_groups_with_same_prefix=True, keep_only_best_group=True,
                    desc_stats=gene_desc.stats)])
                if proc_sent:
                    joined_sent.append(proc_sent)
                comp_sent = " and ".join([sentence.text for sentence in go_sentences.get_sentences(
                    aspect='C', merge_groups_with_same_prefix=True, keep_only_best_group=True,
                    desc_stats=gene_desc.stats)])
                if comp_sent:
                    joined_sent.append(comp_sent)
                colocalizes_with_comp_sent = " and ".join([sentence.text for sentence in go_sentences.get_sentences(
                    aspect='C', qualifier='colocalizes_with', merge_groups_with_same_prefix=True,
                    keep_only_best_group=True, desc_stats=gene_desc.stats)])
                if colocalizes_with_comp_sent:
                    joined_sent.append(colocalizes_with_comp_sent)

            # exclude disease module for now

            # do_sentences = generate_sentences(df.get_annotations(
            #     geneid=gene.id, annot_type=AnnotationType.DO,
            #     priority_list=self.do_annotations_priority,
            #     desc_stats=gene_desc.stats), ontology=df.get_do_ontology(),
            #     evidence_groups_priority_list=self.do_evidence_groups_priority_list,
            #     prepostfix_sentences_map=self.do_prepostfix_sentences_map,
            #     evidence_codes_groups_map=self.do_evidence_codes_groups_map,
            #     remove_parent_terms=True,
            #     merge_num_terms_threshold=3,
            #     merge_min_distance_from_root=self.do_trim_min_distance_from_root,
            #     desc_stats=gene_desc.stats,
            #     truncate_others_generic_word=self.do_truncate_others_aggregation_word,
            #     truncate_others_aspect_words=self.do_truncate_others_terms)
            # if do_sentences:
            #     disease_sent = " and ".join([sentence.text for sentence in do_sentences.get_sentences(
            #         aspect='D', merge_groups_with_same_prefix=True, keep_only_best_group=False,
            #         desc_stats=gene_desc.stats)])
            #
            #    if disease_sent:
            #        joined_sent.append(disease_sent)

            if len(joined_sent) > 0:
                desc = "; ".join(joined_sent) + "."
                if len(desc) > 0:
                    gene_desc.description = desc[0].upper() + desc[1:]
                else:
                    gene_desc.description = None
            else:
                gene_desc.description = None
            desc_writer.add_gene_desc(gene_desc)

        desc_writer.write(self.graph)
