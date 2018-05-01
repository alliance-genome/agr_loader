from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.descriptions_rules import GeneDesc, generate_go_sentences
from services.gene_descriptions.data_fetcher import Neo4jDataFetcher
from services.gene_descriptions.descriptions_writer import Neo4jGDWriter


class GeneDescGenerator(object):

    def __init__(self, config_file_path: str, go_dataset, graph_db):
        self.conf_parser = GenedescConfigParser(config_file_path)
        self.exclusion_list = self.conf_parser.get_go_terms_exclusion_list()
        self.go_prepostfix_sentences_map = self.conf_parser.get_go_prepostfix_sentences_map()
        self.go_prepostfix_special_cases_sent_map = self.conf_parser.get_go_prepostfix_special_cases_sent_map()
        self.go_annotations_priority = self.conf_parser.get_go_annotations_priority()
        self.evidence_codes_groups_map = self.conf_parser.get_evidence_codes_groups_map()
        self.evidence_groups_priority_list = self.conf_parser.get_evidence_groups_priority_list()
        self.go_terms_replacement_dict = self.conf_parser.get_go_rename_terms()
        self.go_truncate_others_aggregation_word = self.conf_parser.get_go_truncate_others_aggregation_word()
        self.go_truncate_others_terms = self.conf_parser.get_go_truncate_others_terms()
        self.go_trim_min_distance_from_root = self.conf_parser.get_go_trim_min_distance_from_root()
        self.cached_go_ontology = None
        self.graph = graph_db
        self.go_dataset = go_dataset

    def generate_descriptions(self, go_annotations, data_provider):
        # Generate gene descriptions and save to db
        desc_writer = Neo4jGDWriter()

        df = Neo4jDataFetcher(go_terms_exclusion_list=self.exclusion_list,
                              go_terms_replacement_dict=self.go_terms_replacement_dict,
                              db_graph=self.graph, go_ontology=self.cached_go_ontology,
                              data_provider=data_provider)
        df.load_go_data(go_terms_list=self.go_dataset, go_annotations=go_annotations)
        # load go ontology only for the first data provider, use cached data for the others
        if not self.cached_go_ontology:
            self.cached_go_ontology = df.get_go_ontology()
        for gene in df.get_gene_data():
            gene_desc = GeneDesc(gene_id=gene.id, gene_name=gene.name)
            sentences = generate_go_sentences(df.get_go_annotations(
                gene.id, priority_list=self.go_annotations_priority, desc_stats=gene_desc.stats),
                go_ontology=df.get_go_ontology(),
                evidence_groups_priority_list=self.evidence_groups_priority_list,
                go_prepostfix_sentences_map=self.go_prepostfix_sentences_map,
                go_prepostfix_special_cases_sent_map=self.go_prepostfix_special_cases_sent_map,
                evidence_codes_groups_map=self.evidence_codes_groups_map,
                remove_parent_terms=True,
                merge_num_terms_threshold=3,
                merge_min_distance_from_root=self.go_trim_min_distance_from_root,
                desc_stats=gene_desc.stats, go_terms_replacement_dict=self.go_terms_replacement_dict,
                truncate_others_generic_word=self.go_truncate_others_aggregation_word,
                truncate_others_aspect_words=self.go_truncate_others_terms)
            if sentences:
                joined_sent = []
                func_sent = " and ".join([sentence.text for sentence in sentences.get_sentences(
                    go_aspect='F', merge_groups_with_same_prefix=True, keep_only_best_group=True,
                    desc_stats=gene_desc.stats)])
                if func_sent:
                    joined_sent.append(func_sent)
                proc_sent = " and ".join([sentence.text for sentence in sentences.get_sentences(
                    go_aspect='P', merge_groups_with_same_prefix=True, keep_only_best_group=True,
                    desc_stats=gene_desc.stats)])
                if proc_sent:
                    joined_sent.append(proc_sent)
                comp_sent = " and ".join([sentence.text for sentence in sentences.get_sentences(
                    go_aspect='C', merge_groups_with_same_prefix=True, keep_only_best_group=True,
                    desc_stats=gene_desc.stats)])
                if comp_sent:
                    joined_sent.append(comp_sent)

                go_desc = "; ".join(joined_sent) + "."
                if len(go_desc) > 0:
                    gene_desc.description = go_desc[0].upper() + go_desc[1:]
            else:
                gene_desc.description = "No description available"
            desc_writer.add_gene_desc(gene_desc)

        desc_writer.write(self.graph)
