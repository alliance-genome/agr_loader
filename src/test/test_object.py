# Used for loading a test subset of data for AGR.
# Note: When testing is enabled, GO annotations and GO terms are only loaded for the following testIdSet.
# The testIdSet is used to "filter" these entries in the appropriate extractor files. 

class TestObject(object):

    def __init__(self, useTestObject):
        # TODO Separate gene ids from other forms of id?
        self.testIdSet = {'HGNC:17889', 'HGNC:25818', 'HGNC:3686', 'HGNC:7881',
            'RGD:70891', 'RGD:1306349', 'RGD:620796',
            'MGI:109337', 'MGI:108202', 'MGI:2676586', 'MGI:88180', 'MGI:109583', 'MGI:96765', 'MGI:1916172', 'MGI:96680', 'MGI:2175810', 'MGI:5437110', 'MGI:5437116'
            'ZFIN:ZDB-GENE-990415-72', 'ZFIN:ZDB-GENE-030131-3445', 'ZFIN:ZDB-GENE-980526-388', 'ZFIN:ZDB-GENE-010525-1','ZFIN:ZDB-FISH-150901-29235',
            'FB:FBgn0083973', 'FB:FBgn0037960', 'FB:FBgn0027296', 'FB:FBgn0033885', 'FB:FBgn0024320', 'FB:FBgn0283499',
            'WB:WBGene00044305', 'WB:WBGene00169423', 'WB:WBGene00000987',
            'SGD:S000003256', 'SGD:S000003513', 'SGD:S000000119', 'SGD:S000001015'}

        self.useTestObject = useTestObject
        self.testGoTerms = []

    def using_test_data(self):
        return self.useTestObject

    def check_for_test_id_entry(self, primaryId):
        if primaryId in self.testIdSet:
            return True
        else:
            return False

    def add_go_ids(self, goIdList):
        self.testGoTerms.extend(goIdList)

    def check_for_test_go_entry(self, goTermId):
        if goTermId in self.testGoTerms:
            return True
        else:
            return False