from .mod import MOD

class FlyBase(MOD):

    def __init__(self):
        self.species = "Drosophila melanogaster"
        self.loadFile = "FB_1.0.3_6.tar.gz"
        self.bgiName = "/FB_1.0.3_BGI.json"
        self.diseaseName = "/FB_1.0.3_disease.json"
        self.alleleName = "/FB_1.0.4_allele.json"
        self.geneAssociationFile = "gene_association_1.0.fb.gz"
        self.identifierPrefix = "FB:"

    def load_genes(self, batch_size, testObject):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://flybase.org/reports/" + gene_id + ".html"

    @staticmethod
    def get_organism_names():
        return ["Drosophila melanogaster", "D. melanogaster", "DROME"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots_mod(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots_mod(self, self.diseaseName)
        return gene_disease_dict

    def load_disease_objects(self, batch_size, testObject):
        data = MOD.load_disease_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data

    def load_allele_objects(self, batch_size, testObject):
        data = ""
            #MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile)
        return data
