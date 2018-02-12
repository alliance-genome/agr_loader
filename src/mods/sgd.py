from .mod import MOD

class SGD(MOD):

    def __init__(self):
        self.species = "Saccharomyces cerevisiae"
        self.loadFile = "SGD_1.0.3.tar.gz"
        self.bgiName = "/SGD_1.0.3/SGD_1.0.2_basicGeneInformation.json"
        self.diseaseName = "/SGD_1.0.3/disease_association.SGD.1.0.2.json"
        self.alleleName = "/SGD_1.0.4_allele.json"
        self.geneAssociationFile = "gene_association_1.0.sgd.gz"
        self.identifierPrefix = "SGD:"

    def load_genes(self, batch_size, testObject):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.yeastgenome.org/locus/" + gene_id + "/overview"

    @staticmethod
    def get_organism_names():
        return ["Saccharomyces cerevisiae", "S. cerevisiae", "YEAST"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots_mod(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_disease_gene_objects(self, batch_size, testObject):
        data = MOD.load_disease_gene_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data

# these are commented out because SGD has no allele data and no allele->disease data right now

    def load_disease_feature_objects(self, batch_size, testObject, graph):
        data = ""
            #MOD.load_disease_feature_objects_mod(batch_size, testObject, SGD.diseaseName, SGD.loadFile, graph)
        return data

    def load_allele_objects(self, batch_size, testObject):
        data = ""
            #MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile)
        return data
