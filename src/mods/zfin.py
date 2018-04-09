from .mod import MOD

class ZFIN(MOD):

    def __init__(self):
        self.species = "Danio rerio"
        self.loadFile = "ZFIN_1.0.0.0_2.tar.gz"
        self.bgiName = "/ZFIN_1.0.0.0_1_BGI.json"
        self.diseaseName = "/ZFIN_1.0.0.0_1_disease.json"
        self.alleleName = "/ZFIN_1.0.0.0_1_allele.json"
        self.geneAssociationFile = "gene_association_1.0.zfin.gz"
        self.identifierPrefix = "ZFIN:"
        self.species = "Danio+rerio"


    def load_genes(self, batch_size, testObject, graph):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile, graph)
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

    def load_disease_gene_objects(self, batch_size, testObject):
        data = MOD.load_disease_gene_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data

    def load_disease_allele_objects(self, batch_size, testObject, graph):
        data = MOD.load_disease_allele_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile, graph)
        return data

    def load_allele_objects(self, batch_size, testObject, graph):
        data = MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile, graph)
        return data

    def extract_geo_entrez_ids(self):
        entrezIds = MOD.extract_entrez_ids_from_geo(self.geoSpecies)
        return entrezIds