from .mod import MOD


class WormBase(MOD):

    def __init__(self):
        self.species = "Caenorhabditis elegans"

        self.loadFile = "WB_1.0.0.7_1.tar.gz"
        self.bgiName = "/WB_1.0.0.7_BGI.json"
        self.diseaseName = "/WB_1.0.0.7_disease.json"
        self.phenotypeName = "/WB_1.0.0.7_phenotype.json"
        self.alleleName = "/WB_1.0.0.7_allele.json"
        self.wtExpressionName= "/WB_1.0.0.7_expression.json"
        self.geneAssociationFile = "gene_association_1.7.wb.gz"

        self.identifierPrefix = "WB:"
        self.geoSpecies = "Caenorhabditis+elegans"
        self.geoRetMax = "30000"
        self.dataProvider = "WB"

    def load_genes(self, batch_size, testObject, graph, species):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile, species)
        return data

    @staticmethod
    def get_organism_names():
        return ["Caenorhabditis elegans", "C. elegans", "CAEEL"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots_mod(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_disease_gene_objects(self, batch_size, testObject, species):
        data = MOD.load_disease_gene_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile, species)
        return data

    def load_disease_allele_objects(self, batch_size, testObject, graph, species):
        data = MOD.load_disease_allele_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile, graph, species)
        return data

    def load_allele_objects(self, batch_size, testObject, species):
        data = MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile, species)
        return data

    def load_phenotype_objects(self, batch_size, testObject, species):
        data = MOD.load_phenotype_objects_mod(self, batch_size, testObject, self.phenotypeName, self.loadFile, species)
        return data

    def load_wt_expression_objects(self, batch_size, testObject, species):
        data = MOD.load_wt_expression_objects_mod(self, batch_size, testObject, self.wtExpressionName, self.loadFile)
        return data


    def extract_geo_entrez_ids_from_geo(self, graph):
        xrefs = MOD.extract_geo_entrez_ids_from_geo(self, self.geoSpecies, self.geoRetMax, graph)
        return xrefs