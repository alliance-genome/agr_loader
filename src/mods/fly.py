from .mod import MOD

class FlyBase(MOD):

    def __init__(self):
        self.species = "Drosophila melanogaster"
        self.loadFile = "FB_1.0.0.4_3.tar.gz"
        self.bgiName = "/FB_1.0.0.4_BGI.json"
        self.diseaseName = "/FB_1.0.0.4_disease.json"
        self.phenotypeName = "/FB_1.0.0.4_phenotype.json"
        self.alleleName = "/FB_1.0.0.4_feature.json"
        self.wtExpressionName = "/FB_1.0.0.4_expression.json"
        self.geneAssociationFile = "gene_association_1.7.fb.gz"
        self.identifierPrefix = "FB:"
        self.geoSpecies = "Drosophila+melanogaster"
        self.geoRetMax = "20000"
        self.dataProvider = "FB"

    def load_genes(self, batch_size, testObject, graph, species):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile,species)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://flybase.org/reports/" + gene_id + ".html"

    @staticmethod
    def get_organism_names():
        return ["Drosophila melanogaster", "D. melanogaster", "DROME"]

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
