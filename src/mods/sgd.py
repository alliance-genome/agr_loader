from .mod import MOD

class SGD(MOD):

    def __init__(self):
        self.species = "Saccharomyces cerevisiae"
        self.loadFile = "SGD_0.6.0_1.tar.gz"
        self.bgiName = "/SGD_0.6_basicGeneInformation.json"
        self.diseaseName = "/SGD_0.6_diseaseAssociation.json"
        self.geneAssociationFile = "gene_association.sgd.gz"

    def load_genes(self, batch_size, test_set):
        data = MOD().load_genes(batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.yeastgenome.org/locus/" + gene_id + "/overview"

    @staticmethod
    def get_organism_names():
        return ["Saccharomyces cerevisiae", "S. cerevisiae", "YEAST"]

    def load_go_prefix(self):
        go_annot_dict = MOD.load_go_prefix(self.geneAssociationFile, self.species)
        return go_annot_dict

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self.diseaseName)
        return gene_disease_dict