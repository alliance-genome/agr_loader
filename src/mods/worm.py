from .mod import MOD

class WormBase(MOD):

    def __init__(self):
        self.species = "Caenorhabditis elegans"
        self.loadFile = "WB_0.6.1_1.tar.gz"
        self.bgiName = "/WB_0.6.1_BGI.json"
        self.diseaseName = "/WB_0.6.1_disease.json"
        self.geneAssociationFile = "gene_association.wb.gz"
        self.identifierPrefix = "WB:"

    def load_genes(self, batch_size, test_set):
        data = MOD.load_genes(self, batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.wormbase.org/species/c_elegans/gene/" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Caenorhabditis elegans", "C. elegans", "CAEEL"]

    def load_go_annots(self):
        go_annot_list = MOD.load_go_annots(self, self.geneAssociationFile, self.species, self.identifierPrefix)
        return go_annot_list

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self, self.diseaseName)
        return gene_disease_dict