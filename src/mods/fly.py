from .mod import MOD

class FlyBase(MOD):

    def __init__(self):
        self.species = "Drosophila melanogaster"
        self.loadFile = "FB_0.6.2_3.tar.gz"
        self.bgiName = "/FB_0.6_basicGeneInformation.json"
        self.diseaseName = "/FB_0.6_diseaseAnnotations.json"
        self.geneAssociationFile = "gene_association.fb.gz"
        self.identifierPrefix = "FB:"

    def load_genes(self, batch_size, test_set):
        data = MOD.load_genes(self, batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://flybase.org/reports/" + gene_id + ".html"

    @staticmethod
    def get_organism_names():
        return ["Drosophila melanogaster", "D. melanogaster", "DROME"]

    def load_go_annots(self):
        go_annot_list = MOD.load_go_annots(self, self.geneAssociationFile, self.species, self.identifierPrefix)
        return go_annot_list

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots(self, self.diseaseName)
        return gene_disease_dict