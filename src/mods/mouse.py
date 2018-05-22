import pprint
from .mod import MOD

class MGI(MOD):

    def __init__(self):
        self.species = "Mus musculus"
        self.loadFile = "MGI_1.0.0.3_1.tar.gz"
        self.bgiName = "/MGI_1.0.0.3_BGI.json"
        self.alleleName = "/MGI_1.0.0.3_allele.json"
        self.diseaseName = "/MGI_1.0.0.3_disease.json"
        self.phenotypeName = "/MGI_1.0.0.3_phenotype.json"
        self.geneAssociationFile = "gene_association_1.0.mgi.gz"
        self.geoSpecies = 'Mus+musculus'
        self.geoRetMax = "50000"
        self.identifierPrefix = "" # None for MGI.

    def load_genes(self, batch_size, testObject, graph):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.informatics.jax.org/marker/" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Mus musculus", "M. musculus", "MOUSE"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots_mod(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_disease_gene_objects(self, batch_size, testObject):
        data = MOD.load_disease_gene_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data

    def load_disease_allele_objects(self, batch_size, testObject, graph):
        data = MOD.load_disease_allele_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile, graph)
        return data

    def load_phenotype_objects(self, batch_size, testObject):
        data = MOD.load_phenotype_objects_mod(self, batch_size, testObject, self.phenotypeName, self.loadFile)
        return data

    def load_allele_objects(self, batch_size, testObject):
        data = MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile)
        return data

    def extract_geo_entrez_ids_from_geo(self, graph):
        xrefs = MOD.extract_geo_entrez_ids_from_geo(self, self.geoSpecies, self.geoRetMax, graph)
        # pprint.pprint("these are mouse xrefs")
        # for xref in xrefs:
        #     pprint.pprint(xref)
        return xrefs