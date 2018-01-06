from .mod import MOD

class RGD(MOD):

    def __init__(self):
        self.species = "Rattus norvegicus"
        self.loadFile = "RGD_1.0.3_7.tar.gz"
        self.bgiName = "/RGD_1.0.3_BGI.10116.json"
        self.diseaseName = "/RGD_1.0.3_disease.10116.daf.json"
        self.alleleName = "/RGD_1.0.4_allele.json"
        self.geneAssociationFile = "gene_association_1.0.rgd.gz"
        self.identifierPrefix = "RGD:"

    def load_genes(self, batch_size, testObject):
        data = MOD.load_genes_mod(self, batch_size, testObject, self.bgiName, self.loadFile)
        return data

    @staticmethod
    def gene_href(gene_id):
        return "http://www.rgd.mcw.edu/rgdweb/report/gene/main.html?id=" + gene_id

    @staticmethod
    def get_organism_names():
        return ["Rattus norvegicus", "R. norvegicus", "RAT"]

    def extract_go_annots(self, testObject):
        go_annot_list = MOD.extract_go_annots_mod(self, self.geneAssociationFile, self.species, self.identifierPrefix, testObject)
        return go_annot_list

    def load_do_annots(self):
        gene_disease_dict = MOD.load_do_annots_mod(self, self.diseaseName)
        return gene_disease_dict

    def load_disease_objects(self, batch_size, testObject):
        data = MOD.load_disease_objects_mod(self, batch_size, testObject, self.diseaseName, self.loadFile)
        return data

    def load_allele_objects(self, batch_size, testObject):
        data = ""
            #MOD.load_allele_objects_mod(self, batch_size, testObject, self.alleleName, self.loadFile)
        return data
