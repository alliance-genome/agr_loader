# Used for loading a test subset of data for AGR.
# Note: When testing is enabled, GO annotations and GO terms are only loaded for the following testIdSet.
# The testIdSet is used to "filter" these entries in the appropriate extractor files. 

class TestObject(object):

    def __init__(self, useTestObject):
        # TODO Separate gene ids from other forms of id?
        self.testIdSet = {'HGNC:17889', 'HGNC:25818', 'HGNC:3686', 'HGNC:7881','HGNC:24502',
            'RGD:70891', 'RGD:1306349', 'RGD:620796','RGD:61995',
            'MGI:5437116', 'MGI:109337', 'MGI:108202', 'MGI:2676586', 'MGI:88180', 'MGI:109583', 'MGI:96765', 'MGI:1099804', 'MGI:109583',
            'MGI:1916172', 'MGI:96680', 'MGI:2175810', 'MGI:5437110', 'MGI:5437073','MGI:88525', 'MGI:1923696',
            'ZFIN:ZDB-GENE-990415-72', 'ZFIN:ZDB-GENE-030131-3445', 'ZFIN:ZDB-GENE-980526-388', 'ZFIN:ZDB-GENE-010525-1',
            'ZFIN:ZDB-FISH-150901-29235', 'ZFIN:ZDB-GENE-060117-5',
            'FB:FBgn0083973', 'FB:FBgn0037960', 'FB:FBgn0027296', 'FB:FBgn0033885', 'FB:FBgn0024320', 'FB:FBgn0283499', 'FB:FBgn0285944',
            'WB:WBGene00044305', 'WB:WBGene00169423', 'WB:WBGene00000987', 'WB:WBGene00015146','WB:WBGene00000898',
            'SGD:S000003256', 'SGD:S000003513', 'SGD:S000000119', 'SGD:S000001015'}

        self.useTestObject = useTestObject
        self.testOntologyTerms = {'DOID:0110741','DOID:0110739','DOID:10021','DOID:10030','DOID:0001816','DOID:0060171','DOID:1115','DOID:0001816','DOID:14330','DOID:9452','DOID:9455','DOID:1059',
            'GO:0019899','GO:0005515','GO:0043393','GO:0022607','GO:0009952','GO:0005764','GO:0060271','GO:0048263','GO:0007492','GO:0030902',
            'GO:0070121','GO:0030901','GO:0030182','GO:0042664','GO:0030916','GO:0021571','GO:0061195', 'GO:0048705','GO:0030335','GO:0048709'}

    def using_test_data(self):
        return self.useTestObject

    def check_for_test_id_entry(self, primaryId):
        if primaryId in self.testIdSet:
            return True
        else:
            return False

    def add_ontology_ids(self, oIdList):
        self.testOntologyTerms.update(oIdList)

    def check_for_test_ontology_entry(self, termId):
        if termId in self.testOntologyTerms:
            return True
        else:
            return False
