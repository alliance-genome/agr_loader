from .mod import MOD

class ZFIN(MOD):

    def __init__(self):
        self.species = "Danio rerio"
        self.loadFile = "ZFIN_1.0.0.4_3.tar.gz"
        self.bgiName = "/ZFIN_1.0.0.4_1_BGI.json"
        self.diseaseName = "/ZFIN_1.0.0.4_1_disease.json"
        self.phenotypeName = "/ZFIN_1.0.0.4_1_phenotype.json"
        self.alleleName = "/ZFIN_1.0.0.4_1_allele.json"
        self.geneAssociationFile = "gene_association_1.7.zfin.gz"

        self.identifierPrefix = "ZFIN:"
        self.geoSpecies = "Danio+rerio"
        self.geoRetMax = "100000"
        self.dataProvider = "ZFIN"

    def load_genes(self, batch_size, testObject, graph, species):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile, species)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://zfin.org/" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Danio rerio", "D. rerio", "DANRE"]

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

    def extract_geo_entrez_ids_from_geo(self, graph):
        xrefs = MOD.extract_geo_entrez_ids_from_geo(self, self.geoSpecies, self.geoRetMax, graph)
        # pprint.pprint("these are mouse xrefs")
        # for xref in xrefs:
        #     pprint.pprint(xref)
        return xrefs