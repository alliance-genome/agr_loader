from .mod import MOD

class Human(MOD):

    def __init__(self):
        self.species = "Homo sapiens"
        self.loadFile = "RGD_0.6_1.tar.gz"
        self.bgiName = "/RGD_0.6.2_basicGeneInformation.9606.json"
        self.diseaseName = "/RGD_0.6.2_disease.9606.daf.json"
        self.geneAssociationFile = "gene_association.human.gz"

    def load_genes(self, batch_size, test_set):
        data = MOD.load_genes(self, batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Homo sapiens", "H. sapiens", "HUMAN"]

    def load_go(self):
        go_annot_dict = MOD.load_go(self, self.geneAssociationFile, self.species)
        return go_annot_dict

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self, self.diseaseName)
        return gene_disease_dict

    def load_disease_objects(self, batch_size, test_set):
        data = MOD.load_disease_objects(self, batch_size, test_set, self.diseaseName, self.loadFile)
        return data