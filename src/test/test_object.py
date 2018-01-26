# Used for loading a test subset of data for AGR.
# Note: When testing is enabled, GO annotations and GO terms are only loaded for the following testIdSet.
# The testIdSet is used to "filter" these entries in the appropriate extractor files. 

class TestObject(object):

    def __init__(self, useTestObject):
        # TODO Separate gene ids from other forms of id?

        self.testIdSet = {'HGNC:17889', 'HGNC:25818', 'HGNC:3686', 'HGNC:7881', 'HGNC:6709', 'HGNC:6526', 'HGNC:6553',
                          'HGNC:6560','HGNC:6551',
                          'RGD:70891', 'RGD:1306349','RGD:708528', 'RGD:620796', 'RGD:61995', 'RGD:1309165','RGD:1581495',
                          'RGD:2322065','RGD:1309063','RGD:2845','RGD:628748','RGD:1581476',
                          'RGD:1309312', 'RGD:7627512', 'RGD:1309105', 'RGD:1309109', 'RGD:7627503','RGD:1578801',
                          'MGI:5437116', 'MGI:1915135', 'MGI:109337', 'MGI:108202', 'MGI:2676586', 'MGI:88180','MGI:88467',
                          'MGI:109583', 'MGI:96765', 'MGI:1099804',
                          'MGI:1916172', 'MGI:96680', 'MGI:2175810', 'MGI:5437110', 'MGI:5437073', 'MGI:88525',
                          'MGI:1923696', 'MGI:1929597','MGI:87853','MGI:2179405',
                          'MGI:1337006', 'MGI:1929470', 'MGI:1929288', 'MGI:1929209', 'MGI:1915127', 'MGI:1915121',
                          'MGI:1915122', 'MGI:1915123', 'MGI:3643831',
                          'MGI:1929183', 'MGI:2443198', 'MGI:1861441', 'HGNC:7218', 'MGI:1928478', 'MGI:1928761',
                          'MGI:1914047', 'MGI:88053', 'MGI:88054','MGI:1855948',
                          'MGI:88056', 'MGI:88456', 'MGI:88447', 'MGI:88587', 'MGI:1338803', 'MGI:94864', 'MGI:1915101',
                          'MGI:1915112','MGI:1355324','MGI:3029164',
                          'MGI:1915181', 'MGI:1915162', 'MGI:1915164', 'MGI:1929699',
                          'ZFIN:ZDB-GENE-990415-72', 'ZFIN:ZDB-GENE-030131-3445', 'ZFIN:ZDB-GENE-980526-388',
                          'ZFIN:ZDB-GENE-010525-1','ZFIN:ZDB-GENE-010525-1','ZFIN:ZDB-GENE-060117-5',
                          'ZFIN:ZDB-GENE-050302-80', 'ZFIN:ZDB-GENE-060503-876',
                          'ZFIN:ZDB-GENE-050302-82', 'ZFIN:ZDB-GENE-030131-4430','ZFIN:ZDB-GENE-060503-872',
                          'ZFIN:ZDB-GENE-060503-873','ZFIN:ZDB-GENE-010525-1',
                          'ZFIN:ZDB-GENE-060503-867', 'ZFIN:ZDB-GENE-010323-11','ZFIN:ZDB-GENE-010525-1',
                          'ZFIN:ZDB-GENE-010320-1','ZFIN:ZDB-GENE-010525-1','ZFIN:ZDB-GENE-051127-5','ZFIN:ZDB-GENE-990415-270',
                          'FB:FBgn0083973', 'FB:FBgn0037960', 'FB:FBgn0027296','FB:FBgn0032006','FB:FBgn0001319','FB:FBgn0002369',
                          'FB:FBgn0033885', 'FB:FBgn0024320', 'FB:FBgn0283499',
                          'FB:FBgn0032465', 'FB:FBgn0285944', 'FB:FBgn0032728','FB:FBgn0000014',
                          'FB:FBgn0032729', 'FB:FBgn0065610', 'FB:FBgn0032730', 'FB:FBgn0032732','FB:FBgn0260987',
                          'FB:FBgn0032781', 'FB:FBgn0032782', 'FB:FBgn0032740',
                          'FB:FBgn0032741', 'FB:FBgn0032744', 'FB:FBgn0036309',
                          'WB:WBGene00044305', 'WB:WBGene00169423', 'WB:WBGene00000987','WB:WBGene00021789','WB:WBGene00006750', 'WB:WBGene00000540','WB:WBGene00017866','WB:WBGene00001131',
                          'WB:WBGene00015146', 'WB:WBGene00015599', 'WB:WBGene00001133',
                          'SGD:S000003256', 'SGD:S000003513', 'SGD:S000000119', 'SGD:S000001015',
                          'SGD:S000002200', 'SGD:S000002386',
                          'ZFIN:ZDB-ALT-980203-985', 'ZFIN:ZDB-ALT-060608-195','ZFIN:ZDB-ALT-050428-6'}


        self.useTestObject = useTestObject
        self.testOntologyTerms = {'DOID:0110741', 'DOID:0110739', 'DOID:10021', 'DOID:10030', 'DOID:0001816',
                                  'DOID:0060171', 'DOID:1115', 'DOID:0001816', 'DOID:14330', 'DOID:9452',
                                  'DOID:9455', 'DOID:1059',
                                  'GO:0019899','GO:0005515','GO:0043393','GO:0022607',
                                  'GO:0009952','GO:0005764', 'GO:0060271', 'GO:0048263',
                                  'GO:0007492','GO:0030902', 'GO:0070121', 'GO:0030901', 'GO:0030182',
                                  'GO:0042664', 'GO:0030916', 'GO:0021571', 'GO:0061195', 'GO:0048705', 'GO:0030335',
                                  'GO:0048709'}

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
