from .mod import MOD

class SGD(MOD):

    def __init__(self):
        self.species = "Saccharomyces cerevisiae"
        self.loadFile = "SGD_1.0.0.4_4.tar.gz"
        self.bgiName = "/SGD_1.0.0.4_4/SGD.1.0.0.4_basicGeneInformation.json"
        self.diseaseName = "/SGD_1.0.0.4_4/SGD_1.0.0.4_disease.daf.json"
        self.phenotypeName = "/SGD_1.0.0.4_4/SGD.1.0.0.4_phenotype.json"
        self.expressionName = "/SGD_1.0.0.4_4/SGD.1.0.0.4_expression.json"
        self.alleleName = ""
        self.wtExpressionName = "/SGD_1.0.0.4_4/SGD.1.0.0.4_expression.json"
        self.geneAssociationFile = "gene_association_1.7.sgd.gz"
        self.identifierPrefix = "SGD:"
        self.geoSpecies = "Saccharomyces+cerevisiae"
        self.geoRetMax = "10000"
        self.dataProvider = "SGD"

    def load_genes(self, batch_size, testObject, graph, species):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile, species)
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

    def load_disease_gene_objects(self, batch_size, testObject, species):
        data = MOD.load_disease_gene_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile, species)
        return data

# these are commented out because SGD has no allele data and no allele->disease data right now

    def load_disease_allele_objects(self, batch_size, testObject, graph, species):
        data = ""
            #MOD.load_disease_allele_objects_mod(batch_size, testObject, SGD.diseaseName, SGD.loadFile, graph, species)
        return data

    def load_allele_objects(self, batch_size, testObject, species):
        data = ""
            #MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile, species)
        return data

    def load_phenotype_objects(self, batch_size, testObject, species):
        data = MOD.load_phenotype_objects_mod(self, batch_size, testObject, self.phenotypeName, self.loadFile, species)
        return data

    def load_wt_expression_objects(self, batch_size, testObject, species):
        (AOExpression, CCExpression, AOQualifier, AOSubstructure, AOSSQualifier, CCQualifier, AOCCExpression) = MOD.load_wt_expression_objects_mod(self, batch_size, testObject, self.wtExpressionName, self.loadFile)
        return AOExpression, CCExpression, AOQualifier, AOSubstructure, AOSSQualifier, CCQualifier, AOCCExpression

    def extract_geo_entrez_ids_from_geo(self, graph):
        xrefs = MOD.extract_geo_entrez_ids_from_geo(self, self.geoSpecies, self.geoRetMax, graph)
        # pprint.pprint("these are mouse xrefs")
        # for xref in xrefs:
        #     pprint.pprint(xref)
        return xrefs