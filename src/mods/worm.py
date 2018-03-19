from .mod import MOD


class WormBase(MOD):

    def __init__(self):
        self.species = "Caenorhabditis elegans"
        self.loadFile = "WB_1.0.0.2_1.tar.gz"
        self.bgiName = "/WB_1.0.0.2_BGI.json"
        self.diseaseName = "/WB_1.0.0.2_disease.json"
        self.alleleName = "/WB_1.0.0.2_feature.json"
        self.geneAssociationFile = "WB_1.0.0.2_gff3.gff3"
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

    def load_disease_allele_objects(self, batch_size, testObject, graph):
        data = MOD.load_disease_allele_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile, graph)
        return data

    def load_allele_objects(self, batch_size, testObject):
        data = MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile)
        return data