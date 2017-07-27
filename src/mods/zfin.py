from .mod import MOD

class ZFIN(MOD):

    def __init__(self):
        self.species = "Danio rerio"
        self.loadFile = "ZFIN_0.6.1_10.tar.gz"
        self.bgiName = "/ZFIN_0.6.1_BGI.json"
        self.diseaseName = "/ZFIN_0.6.1_DAF.json"
        self.geneAssociationFile = "gene_association.zfin.gz"

    def load_genes(self, batch_size, test_set):
        data = MOD.load_genes(self, batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://zfin.org/" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Danio rerio", "D. rerio", "DANRE"]

    def load_go(self):
        go_annot_dict = MOD.load_go(self, self.geneAssociationFile, self.species)
        return go_annot_dict

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self, self.diseaseName)
        return gene_disease_dict