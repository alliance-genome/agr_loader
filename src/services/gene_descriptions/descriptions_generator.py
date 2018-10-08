import datetime
import os
from collections import defaultdict
import boto3
from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.data_fetcher import DataFetcher, DataType
from genedescriptions.descriptions_rules import GeneDesc, SentenceGenerator, generate_orthology_sentence_alliance_human
from services.gene_descriptions.descriptions_writer import Neo4jGDWriter
from services.gene_descriptions.data_extraction_functions import *


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
            "remove_parent_terms": False,
            "merge_num_terms_threshold": 3,
            "merge_min_distance_from_root": self.conf_parser.get_do_trim_min_distance_from_root(),
            "truncate_others_generic_word": self.conf_parser.get_do_truncate_others_aggregation_word(),
            "truncate_others_aspect_words": self.conf_parser.get_do_truncate_others_terms(),
            "add_multiple_if_covers_more_children": True}

    def create_orthology_sentence(self):
        pass

    def set_initial_go_stats(self, gene_desc: GeneDesc, go_annotations, go_sent_generator, go_sent_generator_exp):
        gene_desc.stats.total_number_go_annotations = len(go_annotations)
        gene_desc.stats.set_initial_go_ids_f = list(set().union(
            [elem for key, sets in go_sent_generator.terms_groups[('F', '')].items() for elem in sets if
             ('F', key, '') in
             self.conf_parser.get_go_prepostfix_sentences_map()],
            [elem for key, sets in go_sent_generator.terms_groups[('F', 'contributes_to')].items() for elem in
             sets if
             ('F', key, 'contributes_to') in self.conf_parser.get_go_prepostfix_sentences_map()]))
        gene_desc.stats.set_initial_experimental_go_ids_f = list(set().union(
            [elem for key, sets in go_sent_generator_exp.terms_groups[('F', '')].items() for elem in sets if
             ('F', key, '')
             in self.conf_parser.get_go_prepostfix_sentences_map()],
            [elem for key, sets in go_sent_generator_exp.terms_groups[
                ('F', 'contributes_to')].items() for elem in sets if ('F', key, 'contributes_to') in
             self.conf_parser.get_go_prepostfix_sentences_map()]))
        gene_desc.stats.set_initial_go_ids_p = [elem for key, sets in
                                                go_sent_generator.terms_groups[('P', '')].items() for
                                                elem in sets if ('P', key, '') in
                                                self.conf_parser.get_go_prepostfix_sentences_map()]
        gene_desc.stats.set_initial_experimental_go_ids_p = [elem for key, sets in
                                                             go_sent_generator_exp.terms_groups[('P', '')].items()
                                                             for
                                                             elem in sets if ('P', key, '') in
                                                             self.conf_parser.get_go_prepostfix_sentences_map()]
        gene_desc.stats.set_initial_go_ids_c = list(set().union(
            [elem for key, sets in go_sent_generator.terms_groups[('C', '')].items() for elem in sets if
             ('C', key, '') in
             self.conf_parser.get_go_prepostfix_sentences_map()],
            [elem for key, sets in go_sent_generator.terms_groups[('C', 'colocalizes_with')].items() for elem in
             sets if
             ('C', key, 'colocalizes_with') in self.conf_parser.get_go_prepostfix_sentences_map()]))
        gene_desc.stats.set_initial_experimental_go_ids_c = list(set().union(
            [elem for key, sets in go_sent_generator_exp.terms_groups[('C', '')].items() for elem in sets if
             ('C', key, '')
             in self.conf_parser.get_go_prepostfix_sentences_map()],
            [elem for key, sets in go_sent_generator_exp.terms_groups[('C', 'colocalizes_with')].items() for elem in
             sets if
             ('C', key, 'colocalizes_with') in self.conf_parser.get_go_prepostfix_sentences_map()]))

    def add_go_function_sentence_and_set_final_stats(self, go_sent_generator, go_sent_generator_exp,
                                                     gene_desc: GeneDesc, joined_sent):
        contributes_to_raw_func_sent = go_sent_generator.get_sentences(
            aspect='F', qualifier='contributes_to', merge_groups_with_same_prefix=True, keep_only_best_group=True,
            **self.go_sent_common_props)
        if contributes_to_raw_func_sent:
            raw_func_sent = go_sent_generator_exp.get_sentences(aspect='F', merge_groups_with_same_prefix=True,
                                                                keep_only_best_group=True,
                                                                **self.go_sent_common_props)
        else:
            raw_func_sent = go_sent_generator.get_sentences(aspect='F', merge_groups_with_same_prefix=True,
                                                            keep_only_best_group=True, **self.go_sent_common_props)
        func_sent = " and ".join([sentence.text for sentence in raw_func_sent])
        if func_sent:
            joined_sent.append(func_sent)
            gene_desc.go_function_description = func_sent
        contributes_to_func_sent = " and ".join([sentence.text for sentence in contributes_to_raw_func_sent])
        if contributes_to_func_sent:
            joined_sent.append(contributes_to_func_sent)
            if not gene_desc.go_function_description:
                gene_desc.go_function_description = contributes_to_func_sent
            else:
                gene_desc.go_function_description += "; " + contributes_to_func_sent
            if not gene_desc.go_description:
                gene_desc.go_description = contributes_to_func_sent
            else:
                gene_desc.go_description += "; " + contributes_to_func_sent

        gene_desc.stats.set_final_go_ids_f = list(set().union([term_id for sentence in raw_func_sent for
                                                               term_id in sentence.terms_ids],
                                                              [term_id for sentence in contributes_to_raw_func_sent
                                                               for
                                                               term_id in sentence.terms_ids]))
        gene_desc.stats.set_final_experimental_go_ids_f = list(set().union(
            [term_id for sentence in raw_func_sent for term_id in sentence.terms_ids if
             sentence.evidence_group.startswith("EXPERIMENTAL")],
            [term_id for sentence in contributes_to_raw_func_sent for
             term_id in sentence.terms_ids if
             sentence.evidence_group.startswith("EXPERIMENTAL")]))

    def add_go_process_sentence_and_set_final_stats(self, go_sent_generator, gene_desc: GeneDesc, joined_sent):
        raw_proc_sent = go_sent_generator.get_sentences(aspect='P', merge_groups_with_same_prefix=True,
                                                        keep_only_best_group=True, **self.go_sent_common_props)
        proc_sent = " and ".join([sentence.text for sentence in raw_proc_sent])
        if proc_sent:
            joined_sent.append(proc_sent)
            gene_desc.go_process_description = proc_sent
        gene_desc.stats.set_final_go_ids_p = [term_id for sentence in raw_proc_sent for term_id in
                                              sentence.terms_ids]
        gene_desc.stats.set_final_experimental_go_ids_p = [term_id for sentence in raw_proc_sent for term_id in
                                                           sentence.terms_ids if
                                                           sentence.evidence_group.startswith("EXPERIMENTAL")]

    def add_go_component_sentence_and_set_final_stats(self, go_sent_generator, go_sent_generator_exp,
                                                      gene_desc: GeneDesc, joined_sent):
        colocalizes_with_raw_comp_sent = go_sent_generator.get_sentences(
            aspect='C', qualifier='colocalizes_with', merge_groups_with_same_prefix=True,
            keep_only_best_group=True, **self.go_sent_common_props)
        if colocalizes_with_raw_comp_sent:
            raw_comp_sent = go_sent_generator_exp.get_sentences(aspect='C', merge_groups_with_same_prefix=True,
                                                                keep_only_best_group=True,
                                                                **self.go_sent_common_props)
        else:
            raw_comp_sent = go_sent_generator.get_sentences(aspect='C', merge_groups_with_same_prefix=True,
                                                            keep_only_best_group=True, **self.go_sent_common_props)
        comp_sent = " and ".join([sentence.text for sentence in raw_comp_sent])
        if comp_sent:
            joined_sent.append(comp_sent)
            gene_desc.go_component_description = comp_sent
            if not gene_desc.go_description:
                gene_desc.go_description = comp_sent
            else:
                gene_desc.go_description += " " + comp_sent
        colocalizes_with_comp_sent = " and ".join([sentence.text for sentence in colocalizes_with_raw_comp_sent])
        if colocalizes_with_comp_sent:
            joined_sent.append(colocalizes_with_comp_sent)
            if not gene_desc.go_component_description:
                gene_desc.go_component_description = colocalizes_with_comp_sent
            else:
                gene_desc.go_component_description += "; " + colocalizes_with_comp_sent
            if not gene_desc.go_description:
                gene_desc.go_description = colocalizes_with_comp_sent
            else:
                gene_desc.go_description += "; " + colocalizes_with_comp_sent
        gene_desc.stats.set_final_go_ids_c = list(set().union([term_id for sentence in raw_comp_sent for
                                                               term_id in sentence.terms_ids],
                                                              [term_id for sentence in
                                                               colocalizes_with_raw_comp_sent for
                                                               term_id in sentence.terms_ids]))
        gene_desc.stats.set_final_experimental_go_ids_c = list(
            set().union([term_id for sentence in raw_comp_sent for
                         term_id in sentence.terms_ids if
                         sentence.evidence_group.startswith("EXPERIMENTAL")],
                        [term_id for sentence in colocalizes_with_raw_comp_sent for
                         term_id in sentence.terms_ids if
                         sentence.evidence_group.startswith("EXPERIMENTAL")]))

    @staticmethod
    def set_merged_go_description(gene_desc, joined_sent):
        if len(joined_sent) > 0:
            desc = "; ".join(joined_sent) + "."
            if len(desc) > 0:
                gene_desc.go_description = desc[0].upper() + desc[1:]
            else:
                gene_desc.go_description = None
        else:
            gene_desc.go_description = None

    def add_do_sentence_and_set_stats(self, df: DataFetcher, human, gene_desc: GeneDesc, joined_sent, gene):
        prepostfix_sent_map = self.conf_parser.get_do_prepostfix_sentences_map()
        if human:
            prepostfix_sent_map = self.conf_parser.get_do_prepostfix_sentences_map_humans()
        do_annotations = df.get_annotations_for_gene(gene_id=gene.id, annot_type=DataType.DO,
                                                     priority_list=self.conf_parser.get_do_annotations_priority())
        do_sentence_generator = SentenceGenerator(
            annotations=do_annotations, ontology=df.do_ontology,
            evidence_groups_priority_list=self.conf_parser.get_do_evidence_groups_priority_list(),
            prepostfix_sentences_map=prepostfix_sent_map,
            prepostfix_special_cases_sent_map=None,
            evidence_codes_groups_map=self.conf_parser.get_do_evidence_codes_groups_map())

        raw_disease_sent = do_sentence_generator.get_sentences(aspect='D', merge_groups_with_same_prefix=True,
                                                               keep_only_best_group=False,
                                                               **self.do_sent_common_props)
        disease_sent = "; ".join([sentence.text for sentence in raw_disease_sent])
        if disease_sent and len(disease_sent) > 0:
            gene_desc.do_description = disease_sent[0].upper() + disease_sent[1:]
            joined_sent.append(disease_sent)
        else:
            gene_desc.do_description = None
        gene_desc.stats.total_number_do_annotations = len(do_annotations)
        gene_desc.stats.set_initial_do_ids = [term_id for terms in do_sentence_generator.terms_groups.values() for
                                              tvalues in terms.values() for term_id in tvalues]
        gene_desc.stats.set_final_do_ids = [term_id for sentence in raw_disease_sent for term_id in
                                            sentence.terms_ids]
        if "(multiple)" in disease_sent:
            gene_desc.stats.number_final_do_term_covering_multiple_initial_do_terms = \
                disease_sent.count("(multiple)")

    @staticmethod
    def add_orthology_sentence_and_set_stats(gene_orthologs, gene_best_orthologs, gene, gene_desc: GeneDesc,
                                             joined_sent):
        best_orthologs_ids = [orth[0] for orth in gene_best_orthologs[gene.id]] if gene.id in gene_best_orthologs \
            else []
        best_orthologs = [[orth[0][5:], *orth[1:]] for orth in gene_best_orthologs[gene.id]] if \
            gene.id in gene_best_orthologs else []
        gene_desc.stats.set_best_orthologs = best_orthologs_ids
        excluded_orthologs = True
        if gene.id in gene_orthologs and gene.id in gene_best_orthologs:
            if len(gene_orthologs[gene.id]) == len(gene_best_orthologs[gene.id]):
                excluded_orthologs = False
        if len(best_orthologs) > 0:
            orth_sent = generate_orthology_sentence_alliance_human(best_orthologs, excluded_orthologs)
            if orth_sent:
                joined_sent.append(orth_sent)
                gene_desc.orthology_description = orth_sent

    def generate_descriptions(self, go_annotations, do_annotations, do_annotations_allele, ortho_data, data_provider,
                              cached_data_fetcher, go_ontology_url, go_association_url,
                              do_ontology_url, do_association_url, human=False) -> DataFetcher:
        # Generate gene descriptions and save to db
        json_desc_writer = Neo4jGDWriter()
        if cached_data_fetcher:
            df = cached_data_fetcher
        else:
            df = DataFetcher(go_relations=["subClassOf", "BFO:0000050"], do_relations=None)
            df.set_ontology(ontology_type=DataType.GO, ontology=self.go_ontology,
                            terms_replacement_regex=self.conf_parser.get_go_rename_terms())
            df.set_ontology(ontology_type=DataType.DO, ontology=self.do_ontology, terms_replacement_regex=None)
        df.set_associations(associations_type=DataType.GO,
                            associations=get_go_associations_from_loader_object(go_annotations, self.go_ontology),
                            exclusion_list=self.conf_parser.get_go_terms_exclusion_list())
        df.set_associations(associations_type=DataType.DO,
                            associations=get_do_associations_from_loader_object(do_annotations, do_annotations_allele,
                                                                                self.do_ontology, self.graph,
                                                                                data_provider),
                            exclusion_list=self.conf_parser.get_do_terms_exclusion_list())
        orthologs = get_orthologs_from_loader_object(ortho_data, data_provider, self.graph)
        best_orthologs = get_best_orthologs_for_genes_in_dict(orthologs)
        for gene in get_gene_data_from_neo4j(data_provider=data_provider, graph=self.graph):
            gene_desc = GeneDesc(gene_id=gene.id, gene_name=gene.name)
            joined_sent = []
            go_annotations = df.get_annotations_for_gene(gene_id=gene.id, annot_type=DataType.GO,
                                                         priority_list=self.conf_parser.get_go_annotations_priority())
            go_sent_gen_common_props_exp = self.go_sent_gen_common_props.copy()
            go_sent_gen_common_props_exp["evidence_codes_groups_map"] = {
                evcode: group for evcode, group in self.go_sent_gen_common_props["evidence_codes_groups_map"].items() if
                "EXPERIMENTAL" in self.go_sent_gen_common_props["evidence_codes_groups_map"][evcode]}
            go_sent_generator_exp = SentenceGenerator(annotations=go_annotations, ontology=df.go_ontology,
                                                      **go_sent_gen_common_props_exp)
            go_sent_generator = SentenceGenerator(
                annotations=go_annotations, ontology=df.go_ontology, **self.go_sent_gen_common_props)
            self.set_initial_go_stats(gene_desc=gene_desc, go_annotations=go_annotations,
                                      go_sent_generator=go_sent_generator, go_sent_generator_exp=go_sent_generator_exp)
            self.add_go_function_sentence_and_set_final_stats(go_sent_generator=go_sent_generator,
                                                              go_sent_generator_exp=go_sent_generator_exp,
                                                              gene_desc=gene_desc, joined_sent=joined_sent)
            self.add_go_process_sentence_and_set_final_stats(go_sent_generator=go_sent_generator,
                                                             gene_desc=gene_desc, joined_sent=joined_sent)
            self.add_go_component_sentence_and_set_final_stats(go_sent_generator=go_sent_generator,
                                                               go_sent_generator_exp=go_sent_generator_exp,
                                                               gene_desc=gene_desc, joined_sent=joined_sent)
            self.set_merged_go_description(gene_desc=gene_desc, joined_sent=joined_sent)
            self.add_do_sentence_and_set_stats(df=df, human=human, gene_desc=gene_desc, joined_sent=joined_sent,
                                               gene=gene)
            self.add_orthology_sentence_and_set_stats(gene_best_orthologs=best_orthologs, gene=gene, gene_desc=gene_desc,
                                                      joined_sent=joined_sent)

            if len(joined_sent) > 0:
                desc = "; ".join(joined_sent) + "."
                if len(desc) > 0:
                    gene_desc.description = desc[0].upper() + desc[1:]
                else:
                    gene_desc.description = None
            else:
                gene_desc.description = None
            if (not human and not gene.id.startswith("HGNC")) or (human and gene.id.startswith("HGNC")):
                json_desc_writer.add_gene_desc(gene_desc)

        json_desc_writer.write_neo4j(self.graph)
        if "GENERATE_REPORTS" in os.environ and (os.environ["GENERATE_REPORTS"] == "True"
                                                 or os.environ["GENERATE_REPORTS"] == "true" or
                                                 os.environ["GENERATE_REPORTS"] == "pre-release"):
            gd_file_name = data_provider
            if human:
                gd_file_name = "HUMAN"
            if "RELEASE" in os.environ:
                release_version = ".".join(os.environ["RELEASE"].split(".")[0:2])
            else:
                release_version = "no-version"
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
        return df
