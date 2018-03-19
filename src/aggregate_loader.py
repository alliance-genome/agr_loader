from loaders import *
from loaders.transactions import *
from loaders.allele_loader import *
from extractors.resource_descriptor_ext import *
from loaders.disease_loader import *
from mods import *
from extractors import *
from test import *
import time
from neo4j.v1 import GraphDatabase

class AggregateLoader(object):
    def __init__(self, uri, useTestObject):
        self.graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))
        # Set size of BGI, disease batches extracted from MOD JSON file
        # for creating Python data structure.
        self.batch_size = 5000
        self.mods = [ZFIN(), FlyBase(), RGD(), Human(), SGD(), MGI(), WormBase()]
        #self.mods = [ZFIN()]
        self.testObject = TestObject(useTestObject)
        self.resourceDescriptors = ""

        # Check for the use of test data.
        if self.testObject.using_test_data() == True:
            print("WARNING: Test data load enabled.")
            time.sleep(1)

    def create_indicies(self):
        print("Creating indicies.")
        Indicies(self.graph).create_indicies()

    def load_from_ontologies(self):
        print ("Extracting SO data.")
        self.so_dataset = SOExt().get_data()
        print("Extracting GO data.")
        self.go_dataset = OExt().get_data(self.testObject, "go_1.0.obo", "/GO")
        print("Extracting DO data.")
        self.do_dataset = OExt().get_data(self.testObject, "do_1.0.obo", "/DO")

        print("extracting resource descriptor")
        self.resourceDescriptors = ResourceDescriptor().get_data()


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
            genes = mod.load_genes(self.batch_size, self.testObject)  # generator object

            c = 0
            start = time.time()
            for gene_list_of_entries in genes:
                BGILoader(self.graph).load_bgi(gene_list_of_entries)
                c = c + len(gene_list_of_entries)
            end = time.time()
            print("Average: %sr/s" % (round(c / (end - start), 2)))

        # Loading annotation data for all MODs after completion of BGI data.
        for mod in self.mods:

            print("Loading MOD alleles for %s into Neo4j." % mod.species)
            alleles = mod.load_allele_objects(self.batch_size, self.testObject)
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