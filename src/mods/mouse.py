from .mod import MOD

class MGI(MOD):

    def __init__(self):
        self.species = "Mus musculus"

        self.loadFile = "MGI_1.0.0.7_2.tar.gz"

        self.bgiName = "/MGI_1.0.0.7_BGI.json"
        self.alleleName = "/MGI_1.0.0.7_allele.json"
        self.diseaseName = "/MGI_1.0.0.7_disease.json"
        self.phenotypeName = "/MGI_1.0.0.7_phenotype.json"
        self.wtExpressionName = "/MGI_1.0.0.7_expression.json"

        self.geneAssociationFile = "gene_association_1.7.mgi.gz"
        self.geoSpecies = 'Mus+musculus'
        self.geoRetMax = "50000"
        self.identifierPrefix = "" # None for MGI.
        self.dataProvider = "MGI"

    def load_genes(self, batch_size, testObject, species):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile, species)
        return data

    @staticmethod
    def get_organism_names():
        return ["Mus musculus", "M. musculus", "MOUSE"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots_mod(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_disease_gene_objects(self, batch_size, testObject, species):
        data = MOD.load_disease_gene_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile, species)
        return data

    def load_disease_allele_objects(self, batch_size, testObject, species):
        data = MOD.load_disease_allele_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile, species)
        return data

    def load_allele_objects(self, batch_size, testObject, species):
        data = MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile, species)
        return data

    def load_wt_expression_objects(self, batch_size, testObject, species):
        data = MOD.load_wt_expression_objects_mod(self, batch_size, testObject, self.wtExpressionName, self.loadFile)
        return data

    def extract_geo_entrez_ids_from_geo(self):
        xrefs = MOD.extract_geo_entrez_ids_from_geo(self, self.geoSpecies, self.geoRetMax)
        # pprint.pprint("these are mouse xrefs")
        # for xref in xrefs:
        #     pprint.pprint(xref)
        return xrefs

    def load_phenotype_objects(self, batch_size, testObject, species):
        data = MOD.load_phenotype_objects_mod(self, batch_size, testObject, self.phenotypeName, self.loadFile, species)
        return data