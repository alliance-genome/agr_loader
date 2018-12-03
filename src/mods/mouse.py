from .mod import MOD

class MGI(MOD):

    def __init__(self, batch_size):
        self.species = "Mus musculus"
        super().__init__(batch_size, self.species)
        self.loadFile = "MGI_1.0.0.7_2.tar.gz"
        self.bgiName = "/MGI_1.0.0.7_BGI.json"
        self.alleleName = "/MGI_1.0.0.7_allele.json"
        self.diseaseName = "/MGI_1.0.0.7_disease.json"
        self.phenotypeName = "/MGI_1.0.0.7_phenotype.json"
        self.wtExpressionName = "/MGI_1.0.0.7_expression.json"
        self.geneAssociationFile = "gene_association_1.7.mgi.gz"
        self.geoRetMax = "50000"
        self.identifierPrefix = "" # None for MGI.
        self.dataProvider = "MGI"

    def load_wt_expression_objects(self):
        data = self.load_wt_expression_objects_mod(self.wtExpressionName, self.loadFile)
        return data