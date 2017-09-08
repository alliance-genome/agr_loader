from .mod import MOD

class SGD(MOD):

    def __init__(self):
        self.species = "Saccharomyces cerevisiae"
        self.loadFile = "SGD_0.6.2_1.0.1.tar.gz"
        self.bgiName = "/sgd/SGD_1.0_basicGeneInformation.json"
        self.diseaseName = "/sgd/SGD_1.0_diseaseAssociation.json"
        self.geneAssociationFile = "gene_association.sgd.gz"
        self.identifierPrefix = "SGD:"

    def load_genes(self, batch_size, testObject):
        data = MOD.load_genes(self, batch_size, testObject, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.yeastgenome.org/locus/" + gene_id + "/overview"

    @staticmethod
    def get_organism_names():
        return ["Saccharomyces cerevisiae", "S. cerevisiae", "YEAST"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self, self.diseaseName)
        return gene_disease_dict

    def load_disease_objects(self, batch_size, testObject):
        data = MOD.load_disease_objects(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data