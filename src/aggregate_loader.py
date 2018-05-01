import os

from loaders import *
from loaders.transactions import *
from loaders.allele_loader import *
from loaders.disease_loader import *
from loaders.geo_loader import *
from loaders.resource_descriptor_loader import *
from mods import *
from extractors import *
from test import *
import time
from neo4j.v1 import GraphDatabase
from genedescriptions.config_parser import GenedescConfigParser
from genedescriptions.descriptions_rules import generate_go_sentences
from genedescriptions.descriptions_writer import GeneDesc, JsonGDWriter
from services.gene_descriptions.data_fetcher import Neo4jDataFetcher
from test import TestObject
from services.gene_descriptions.descriptions_writer import Neo4jGDWriter


class AggregateLoader(object):
    def __init__(self, uri, useTestObject):
        self.graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))
        # Set size of BGI, disease batches extracted from MOD JSON file
        # for creating Python data structure.
        self.batch_size = 5000
        self.mods = [ZFIN(), SGD(), WormBase(), MGI(), RGD(), Human(), FlyBase()]
        #self.mods = [WormBase()]
        self.testObject = TestObject(useTestObject, self.mods)

        self.resourceDescriptors = ""
        self.geoMoEntrezIds = ""

        # Check for the use of test data.
        if self.testObject.using_test_data() == True:
            print("WARNING: Test data load enabled.")
            time.sleep(1)

    def create_indicies(self):
        print("Creating indicies.")
        Indicies(self.graph).create_indicies()

    def load_resource_descriptors(self):
        print("extracting resource descriptor")
        self.resourceDescriptors = ResourceDescriptor().get_data()
        print("loading resource descriptor")
        ResourceDescriptorLoader(self.graph).load_resource_descriptor(self.resourceDescriptors)

    def load_from_ontologies(self):
        print("Extracting SO data.")
        self.so_dataset = SOExt().get_data()
        print("Extracting GO data.")
        self.go_dataset = OExt().get_data(self.testObject, "GO/go_1.0.obo")
        print("Extracting DO data.")
        self.do_dataset = OExt().get_data(self.testObject, "DO/do_1.0.obo")
        print("Downloading MI data.")
        self.mi_dataset = MIExt().get_data()
        # #
        print("Loading MI data into Neo4j.")
        MILoader(self.graph).load_mi(self.mi_dataset)
        print("Loading SO data into Neo4j.")
        SOLoader(self.graph).load_so(self.so_dataset)
        print("Loading GO data into Neo4j.")
        GOLoader(self.graph).load_go(self.go_dataset)
        print("Loading DO data into Neo4j.")
        DOLoader(self.graph).load_do(self.do_dataset)

    def load_from_mods(self):
        print("Extracting BGI data from each MOD.")

        for mod in self.mods:
            print("Loading BGI data for %s into Neo4j." % mod.species)
            genes = mod.load_genes(self.batch_size, self.testObject, self.graph)  # generator object

            c = 0
            start = time.time()
            for gene_list_of_entries in genes:
                BGILoader(self.graph).load_bgi(gene_list_of_entries)
                c = c + len(gene_list_of_entries)
            end = time.time()
            print("Average: %sr/s" % (round(c / (end - start), 2)))

        # Load configuration for gene descriptions - same conf for all MODs
        this_dir = os.path.split(__file__)[0]
        # read conf from file
        conf_parser = GenedescConfigParser(os.path.join(this_dir, "services", "gene_descriptions",
                                                        "genedesc_config.yml"))
        exclusion_list = conf_parser.get_go_terms_exclusion_list()
        go_prepostfix_sentences_map = conf_parser.get_go_prepostfix_sentences_map()
        go_prepostfix_special_cases_sent_map = conf_parser.get_go_prepostfix_special_cases_sent_map()
        go_annotations_priority = conf_parser.get_go_annotations_priority()
        evidence_codes_groups_map = conf_parser.get_evidence_codes_groups_map()
        evidence_groups_priority_list = conf_parser.get_evidence_groups_priority_list()
        go_terms_replacement_dict = conf_parser.get_go_rename_terms()
        go_truncate_others_aggregation_word = conf_parser.get_go_truncate_others_aggregation_word()
        go_truncate_others_terms = conf_parser.get_go_truncate_others_terms()
        go_trim_min_distance_from_root = conf_parser.get_go_trim_min_distance_from_root()
        cached_go_ontology = None

        # Loading annotation data for all MODs after completion of BGI data.
        for mod in self.mods:

            print("Loading MOD alleles for %s into Neo4j." % mod.species)
            alleles = mod.load_allele_objects(self.batch_size, self.testObject, self.graph)
            for allele_list_of_entries in alleles:
                AlleleLoader(self.graph).load_allele_objects(allele_list_of_entries)

            print("Loading Orthology data for %s into Neo4j." % mod.species)
            ortholog_data = OrthoExt().get_data(self.testObject, mod.__class__.__name__, self.batch_size) # generator object
            for ortholog_list_of_entries in ortholog_data:
                OrthoLoader(self.graph).load_ortho(ortholog_list_of_entries)

            print("Loading MOD gene disease annotations for %s into Neo4j." % mod.species)
            features = mod.load_disease_gene_objects(self.batch_size, self.testObject)
            for feature_list_of_entries in features:
                DiseaseLoader(self.graph).load_disease_gene_objects(feature_list_of_entries)

            print("Loading MOD allele disease annotations for %s into Neo4j." % mod.species)
            features = mod.load_disease_allele_objects(self.batch_size, self.testObject, self.graph)
            for feature_list_of_entries in features:
                DiseaseLoader(self.graph).load_disease_allele_objects(feature_list_of_entries)

            print("Extracting GO annotations for %s." % mod.__class__.__name__)
            go_annots = mod.extract_go_annots(self.testObject)
            print("Loading GO annotations for %s into Neo4j." % mod.__class__.__name__)
            GOAnnotLoader(self.graph).load_go_annot(go_annots)

            print("Extracting GEO annotations for %s." % mod.__class__.__name__)
            geo_xrefs = mod.extract_geo_entrez_ids_from_geo(self.graph)
            print("Loading GEO annotations for %s." % mod.__class__.__name__)
            GeoLoader(self.graph).load_geo_xrefs(geo_xrefs)

            if mod.dataProvider:
                # Generate gene descriptions and save to db
                desc_writer = Neo4jGDWriter()

                df = Neo4jDataFetcher(go_terms_exclusion_list=exclusion_list,
                                      go_terms_replacement_dict=go_terms_replacement_dict,
                                      db_graph=self.graph, go_ontology=cached_go_ontology,
                                      data_provider=mod.dataProvider)
                df.load_go_data(go_terms_list=self.go_dataset, go_annotations=go_annots)
                # load go ontology only for the first data provider, use cached data for the others
                if not cached_go_ontology:
                    cached_go_ontology = df.get_go_ontology()
                for gene in df.get_gene_data():
                    gene_desc = GeneDesc(gene_id=gene.id, gene_name=gene.name)
                    sentences = generate_go_sentences(df.get_go_annotations(
                        gene.id, priority_list=go_annotations_priority, desc_stats=gene_desc.stats),
                        go_ontology=df.get_go_ontology(),
                        evidence_groups_priority_list=evidence_groups_priority_list,
                        go_prepostfix_sentences_map=go_prepostfix_sentences_map,
                        go_prepostfix_special_cases_sent_map=go_prepostfix_special_cases_sent_map,
                        evidence_codes_groups_map=evidence_codes_groups_map,
                        remove_parent_terms=True,
                        merge_num_terms_threshold=3,
                        merge_min_distance_from_root=go_trim_min_distance_from_root,
                        desc_stats=gene_desc.stats, go_terms_replacement_dict=go_terms_replacement_dict,
                        truncate_others_generic_word=go_truncate_others_aggregation_word,
                        truncate_others_aspect_words=go_truncate_others_terms)
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

    def load_additional_datasets(self):
            print("Extracting and Loading IMEX data.")
            imex_data = IMEXExt().get_data(self.batch_size)
            for imex_list_of_entries in imex_data:
                IMEXLoader(self.graph).load_imex(imex_list_of_entries)