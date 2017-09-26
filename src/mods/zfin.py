from .mod import MOD

class ZFIN(MOD):

    def __init__(self):
        self.species = "Danio rerio"
        self.loadFile = "ZFIN_1.0_3.tar.gz"
        self.bgiName = "/ZFIN_1.0_basicGeneInformation.json"
        self.diseaseName = "/ZFIN_1.0_disease_daf.json"
        self.geneAssociationFile = "gene_association_1.0.zfin.gz"
        self.identifierPrefix = "ZFIN:"
        
    def load_genes(self, batch_size, test_set):
        data = MOD.load_genes(self, batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://zfin.org/" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Danio rerio", "D. rerio", "DANRE"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self, self.diseaseName)
        return gene_disease_dict

    def load_disease_objects(self, batch_size, test_set):
        data = MOD.load_disease_objects(self, batch_size, test_set, self.diseaseName, self.loadFile)
        return data