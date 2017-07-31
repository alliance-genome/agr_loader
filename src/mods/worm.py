from .mod import MOD

class WormBase(MOD):

    def __init__(self):
        self.species = "Caenorhabditis elegans"
        self.loadFile = "WB_0.6.1_1.tar.gz"
        self.bgiName = "/WB_0.6.1_BGI.json"
        self.diseaseName = "/WB_0.6.1_disease.json"
        self.geneAssociationFile = "gene_association.wb.gz"

    def load_genes(self, batch_size, test_set):
        data = MOD.load_genes(self, batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.wormbase.org/species/c_elegans/gene/" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Caenorhabditis elegans", "C. elegans", "CAEEL"]

    def load_go(self):
        go_annot_dict = MOD.load_go(self, self.geneAssociationFile, self.species)
        return go_annot_dict

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self, self.diseaseName)
        return gene_disease_dict

    def load_disease_objects(self, batch_size, test_set):
        data = MOD.load_disease_objects(self, batch_size, test_set, self.diseaseName, self.loadFile)
        return data