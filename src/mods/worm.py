from .mod import MOD

class WormBase(MOD):

    def __init__(self, batch_size):
        self.species = "Caenorhabditis elegans"
        super().__init__(batch_size, self.species)
        self.loadFile = "WB_1.0.0.7_4.tar.gz"
        self.bgiName = "/WB_1.0.0.7_BGI.json"
        self.diseaseName = "/WB_1.0.0.7_disease.json"
        self.phenotypeName = "/WB_1.0.0.7_phenotype.json"
        self.alleleName = "/WB_1.0.0.7_allele.json"
        self.wtExpressionName= "/WB_1.0.0.7_expression.json"
        self.geneAssociationFile = "gene_association_1.7.wb.gz"
        self.identifierPrefix = "WB:"
        self.geoRetMax = "30000"
        self.dataProvider = "WB"

    def load_wt_expression_objects(self):
        data = self.load_wt_expression_objects_mod(self.wtExpressionName, self.loadFile)
        return data
