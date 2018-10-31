from .mod import MOD

class Human(MOD):

    def __init__(self, batch_size):
        self.species = "Homo sapiens"
        super().__init__(batch_size, self.species)
        self.loadFile = "RGD_1.0.0.7_4.tar.gz"
        self.bgiName = "/RGD_1.0.0.7_BGI.9606.json"
        self.diseaseName = "/RGD_1.0.0.7_disease.9606.json"
        self.phenotypeName = "/RGD_1.0.0.7_phenotype.9606.json"
        self.alleleName = "" # None for Human.
        self.wtExpressionName = "/RGD_1.0.0.7_expression.9606.json"
        self.geneAssociationFile = "gene_association_1.7.1.human.gz"
        self.identifierPrefix = "" # None for Human.
        self.geoRetMax = "40000"
        self.dataProvider = "RGD"

    def load_genes(self):
        data = self.load_genes_mod(self.bgiName, self.loadFile)
        return data

    @staticmethod
    def get_organism_names():
        return ["Homo sapiens", "H. sapiens", "HUMAN"]

    def extract_go_annots(self):
        go_annot_list = self.extract_go_annots_mod(self.geneAssociationFile, self.identifierPrefix)
        return go_annot_list

    def load_disease_allele_objects(self):
        data = ""
        return data

    def load_disease_gene_objects(self):
        data = self.load_disease_gene_objects_mod(self.diseaseName, self.loadFile)
        return data

    def load_phenotype_objects(self):
        data = self.load_phenotype_objects_mod(self.phenotypeName, self.loadFile)
        return data

    def extract_geo_entrez_ids_from_geo(self):
        xrefs = self.extract_geo_entrez_ids_from_geo_mod(self.geoRetMax)
        return xrefs
        
    def extract_ortho_data(self, mod_name):
        data = self.extract_ortho_data_mod(mod_name)
        return data