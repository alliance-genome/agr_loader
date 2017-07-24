from .mod import MOD

class FlyBase(MOD):

    def __init__(self):
        self.species = "Drosophila melanogaster"
        self.loadFile = "FB_0.6.2_3.tar.gz"
        self.bgiName = "/FB_0.6_basicGeneInformation.json"
        self.diseaseName = "/FB_0.6_diseaseAnnotations.json"
        self.geneAssociationFile = "gene_association.fb.gz"

    def load_genes(self, batch_size, test_set):
        data = MOD().load_genes(batch_size, test_set, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://flybase.org/reports/" + gene_id + ".html"

    @staticmethod
    def get_organism_names():
        return ["Drosophila melanogaster", "D. melanogaster", "DROME"]

    def load_go(self):
        go_annot_dict = MOD().load_go(self.geneAssociationFile, self.species)
        return go_annot_dict

    def load_do_annots(self):
        gene_disease_dict = MOD().load_do_annots(self.diseaseName)
        return gene_disease_dict