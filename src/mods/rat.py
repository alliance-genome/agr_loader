from .mod import MOD

class RGD(MOD):

    def __init__(self, batch_size):
        self.species = "Rattus norvegicus"
        super().__init__(batch_size, self.species)
        self.loadFile = "RGD_1.0.0.7_4.tar.gz"
        self.bgiName = "/RGD_1.0.0.7_BGI.10116.json"
        self.diseaseName = "/RGD_1.0.0.7_disease.10116.json"
        self.phenotypeName = "/RGD_1.0.0.7_phenotype.10116.json"
        self.alleleName = "/RGD_1.0.0.7_allele.10116.json"
        self.wtExpressionName = "/RGD_1.0.0.7_expression.10116.json"
        self.geneAssociationFile = "gene_association_1.7.rgd.gz"
        self.identifierPrefix = "RGD:"
        self.geoRetMax = "30000"
        self.dataProvider = "RGD"

    def load_genes(self):
        data = self.load_genes_mod(self.bgiName, self.loadFile)
        return data

    @staticmethod
    def get_organism_names():
        return ["Rattus norvegicus", "R. norvegicus", "RAT"]

    def extract_go_annots(self):
        go_annot_list = self.extract_go_annots_mod(self.geneAssociationFile, self.identifierPrefix)
        return go_annot_list

    def load_disease_gene_objects(self):
        data = self.load_disease_gene_objects_mod(self.diseaseName, self.loadFile)
        return data

    def load_disease_allele_objects(self):
        data = self.load_disease_allele_objects_mod(self.diseaseName, self.loadFile)
        return data

    def load_allele_objects(self):
        data = self.load_allele_objects_mod(self.alleleName, self.loadFile)
        return data

    def load_phenotype_objects(self):
        data = self.load_phenotype_objects_mod(self.phenotypeName, self.loadFile)
        return data

    def load_wt_expression_objects(self):
        data = self.load_wt_expression_objects_mod(self.wtExpressionName, self.loadFile)
        return data

    def extract_geo_entrez_ids_from_geo(self):
        data = self.extract_geo_entrez_ids_from_geo_mod(self.geoRetMax)
        return data
        
    def extract_ortho_data(self, mod_name):
        data = self.extract_ortho_data_mod(mod_name)
        return data