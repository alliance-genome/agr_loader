from .mod import MOD

class SGD(MOD):

    def __init__(self):
        self.species = "Saccharomyces cerevisiae"
        self.loadFile = "SGD_1.0.0.0_1.tar.gz"
        self.bgiName = "/SGD_1.0.0.0_1/SGD_1.0.0.0_BGI.json"
        self.diseaseName = "/SGD_1.0.0.0_1/SGD_1.0.0.0_DAF.json"
        self.alleleName = ""
        self.geneAssociationFile = "gene_association_1.0.sgd.gz"
        self.identifierPrefix = "SGD:"
        self.geoSpecies = "Saccharomyces+cerevisiae"
        self.geoRetMax = "10000"

    def load_genes(self, batch_size, testObject, graph):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile)
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

    def load_disease_gene_objects(self, batch_size, testObject):
        data = MOD.load_disease_gene_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data

# these are commented out because SGD has no allele data and no allele->disease data right now

    def load_disease_allele_objects(self, batch_size, testObject, graph):
        data = ""
            #MOD.load_disease_allele_objects_mod(batch_size, testObject, SGD.diseaseName, SGD.loadFile, graph)
        return data

    def load_allele_objects(self, batch_size, testObject, graph):
        data = ""
            #MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile. graph)
        return data

    def extract_geo_entrez_ids_from_geo(self, graph):
        xrefs = MOD.extract_geo_entrez_ids_from_geo(self, self.geoSpecies, self.geoRetMax, graph)
        # pprint.pprint("these are mouse xrefs")
        # for xref in xrefs:
        #     pprint.pprint(xref)
        return xrefs