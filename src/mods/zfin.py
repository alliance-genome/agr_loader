from .mod import MOD

class ZFIN(MOD):

    def __init__(self, batch_size):
        self.species = "Danio rerio"
        super().__init__(batch_size, self.species)
        self.loadFile = "ZFIN_1.0.0.7_3.tar.gz"
        self.bgiName = "/ZFIN_1.0.0.7_basicGeneInformation.json"
        self.diseaseName = "/ZFIN_1.0.0.7_disease.daf.json"
        self.phenotypeName = "/ZFIN_1.0.0.7_phenotype.json"
        self.alleleName = "/ZFIN_1.0.0.7_allele.json"
        self.wtExpressionName = "/ZFIN_1.0.0.7_expression.json"
        self.geneAssociationFile = "gene_association_1.7.zfin.gz"
        self.identifierPrefix = "ZFIN:"
        self.geoRetMax = "100000"
        self.dataProvider = "ZFIN"

    def load_wt_expression_objects(self):
        data = self.load_wt_expression_objects_mod(self.wtExpressionName, self.loadFile)
        return data