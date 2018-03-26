from .mod import MOD

class MGI(MOD):

    def __init__(self):
        self.species = "Mus musculus"
        self.loadFile = "MGI_1.0.0.0_1.tar.gz"
        self.bgiName = "/MGI_1.0.0.0_BGI.json"
        self.alleleName = "/MGI_1.0.0.0_allele"
        self.diseaseName = "/MGI_1.0.0.0_disease.json"
        self.geneAssociationFile = "gene_association_1.0.mgi.gz"
        self.identifierPrefix = "" # None for MGI.

    def load_genes(self, batch_size, testObject, graph):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile, graph)
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

    def load_allele_objects(self, batch_size, testObject, graph):
        data = MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile, graph)
        return data