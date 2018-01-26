from .mod import MOD

class WormBase(MOD):

    def __init__(self):
        self.species = "Caenorhabditis elegans"
        self.loadFile = "WB_1.0.4_2.tar.gz"
        self.bgiName = "/WB_1.0.3_BGI.json"
        self.diseaseName = "/WB_1.0.3_disease.json"
        self.alleleName = "/WB_1.0.4_feature.json"
        self.geneAssociationFile = "gene_association_1.0.wb.gz"
        self.identifierPrefix = "WB:"

    def load_genes(self, batch_size, testObject):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.wormbase.org/species/c_elegans/gene/" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Caenorhabditis elegans", "C. elegans", "CAEEL"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots_mod(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_disease_gene_objects(self, batch_size, testObject):
        data = MOD.load_disease_gene_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data

    def load_disease_feature_objects(self, batch_size, testObject):
        data = MOD.load_disease_feature_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data

    def load_allele_objects(self, batch_size, testObject):
        data = MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile)
        return data