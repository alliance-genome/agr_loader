from .mod import MOD

class MGI(MOD):

    def __init__(self):
        self.species = "Mus musculus"
        self.loadFile = "MGI_0.6.0_2.tar.gz"
        self.bgiName = "/MGI_0.6_basicGeneInformation.json"
        self.diseaseName = "/MGI_0.6_diseaseAnnotations.json"
        self.geneAssociationFile = "gene_association.mgi.gz"
        self.identifierPrefix = "" # None for MGI.


    def load_genes(self, batch_size, test_set):
        data = MOD.load_genes(self, batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.informatics.jax.org/marker/" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Mus musculus", "M. musculus", "MOUSE"]

    def load_go_annots(self):
        go_annot_list = MOD.load_go_annots(self, self.geneAssociationFile, self.species, self.identifierPrefix)
        return go_annot_list

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self, self.diseaseName)
        return gene_disease_dict

    def load_disease_objects(self, batch_size, test_set):
        data = MOD.load_disease_objects(self, batch_size, test_set, self.diseaseName, self.loadFile)
        return data