from .mod import MOD

class Human(MOD):

    def __init__(self):
        self.species = "Homo sapiens"
        self.loadFile = "RGD_1.0.3_7.tar.gz"
        self.bgiName = "/RGD_1.0_BGI.9606.json"
        self.diseaseName = "/RGD_1.0_disease.9606.daf.json"
        self.geneAssociationFile = "gene_association_1.0.human.gz"
        self.identifierPrefix = "" # None for Human.

    def load_genes(self, batch_size, testObject):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Homo sapiens", "H. sapiens", "HUMAN"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots_mod(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots_mod(self, self.diseaseName)
        return gene_disease_dict

    def load_disease_objects(self, batch_size, testObject):
        data = MOD.load_disease_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data