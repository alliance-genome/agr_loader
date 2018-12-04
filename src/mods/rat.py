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

    def load_wt_expression_objects(self):
        data = self.load_wt_expression_objects_mod(self.wtExpressionName, self.loadFile)
        return data
