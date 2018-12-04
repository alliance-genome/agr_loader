from .mod import MOD

class FlyBase(MOD):

    def __init__(self, batch_size):
        self.species = "Drosophila melanogaster"
        super().__init__(batch_size, self.species)
        self.loadFile = "FB_1.0.0.7_4.tar.gz"
        self.bgiName = "/FB_1.0.0.7_BGI.json"
        self.diseaseName = "/FB_1.0.0.7_disease.json"
        self.phenotypeName = "/FB_1.0.0.7_phenotype.json"
        self.alleleName = "/FB_1.0.0.7_feature.json"
        self.wtExpressionName = "/FB_1.0.0.7_expression.json"
        self.geneAssociationFile = "gene_association_1.7.fb.gz"
        self.identifierPrefix = "FB:"
        self.geoRetMax = "20000"
        self.dataProvider = "FB"

    def load_wt_expression_objects(self):
        data = self.load_wt_expression_objects_mod(self.wtExpressionName, self.loadFile)
        return data