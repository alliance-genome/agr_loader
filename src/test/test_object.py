# Used for loading a test subset of data for AGR.
# Note: When testing is enabled, GO annotations and GO terms are only loaded for the following testIdSet.
# The testIdSet is used to "filter" these entries in the appropriate extractor files. 
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class TestObject(object):

    def __init__(self, useTestObject, modList):
        # TODO Separate gene ids from other forms of id?

        self.modList = modList
        self.useTestObject = useTestObject

        self.mgiIdSet = {
            'MGI:5437116', 'MGI:1915135', 'MGI:109337', 'MGI:108202', 'MGI:2676586', 'MGI:88180', 'MGI:88467',
            'MGI:109583', 'MGI:96765', 'MGI:1099804',
            'MGI:1916172', 'MGI:96680', 'MGI:2175810', 'MGI:5437110', 'MGI:5437073', 'MGI:88525',
            'MGI:1923696', 'MGI:1929597', 'MGI:87853', 'MGI:2179405',
            'MGI:1337006', 'MGI:1929470', 'MGI:1929288', 'MGI:1929209', 'MGI:1915127', 'MGI:1915121',
            'MGI:1915122', 'MGI:1915123', 'MGI:3643831',
            'MGI:1929183', 'MGI:2443198', 'MGI:1861441', 'MGI:1928478', 'MGI:1928761',
            'MGI:1914047', 'MGI:88053', 'MGI:88054', 'MGI:1855948',
            'MGI:88056', 'MGI:88456', 'MGI:88447', 'MGI:88587', 'MGI:1338803', 'MGI:94864', 'MGI:1915101',
            'MGI:1915112', 'MGI:1355324', 'MGI:3029164', 'MGI:1856330'
                                                         'MGI:1915181', 'MGI:1915162', 'MGI:1915164', 'MGI:1929699',
            'MGI:94909', 'MGI:1856331', 'MGI:97490', 'MGI:108092', 'MGI:2156738',
            'MGI:2148260', 'MGI:1856328', 'MGI:2678393', 'MGI:2429942', 'MGI:1856332', 'MGI:5569634',
            'MGI:3531484', 'MGI:3531484',
            'MGI:2148259', 'MGI:3531483', 'MGI:1856329', 'MGI:3531484', 'MGI:5781149', 'MGI:2148259', 'MGI:104735', 'MGI:98834',
            # phenotype objects
            'MGI:2670749', 'MGI:2656842',
            # disease objects
            'MGI:88123', 'MGI:2148259',
            # expression
            'MGI:97570', 'MGI:2181676', 'MGI:1918911', 'MGI:1919311', 'MGI:1920484'
        }

        self.wormbaseIdSet = {
            'WB:WBGene00044305', 'WB:WBGene00169423', 'WB:WBGene00000987', 'WB:WBGene00021789',
            'WB:WBGene00006750', 'WB:WBGene00000540', 'WB:WBGene00017866', 'WB:WBGene00001131',
            'WB:WBGene00015146', 'WB:WBGene00015599', 'WB:WBGene00001133', 'WB:WBGene00001115',
            'WB:WBGene00018468', 'WB:WBGene00019001', 'WB:WBGene00007438', 'WB:WBGene00001136', 'WB:WBGene00006742',
            'WB:WBGene00003883',
            # expression,
            'WB:WBGene00012650', 'WB:Gene00006649', 'WB:WBGene00008117', 'WB:WBGene00004876',
            'WB:WBGene00003883', 'WB:WBGene00006508',
            # phenotype and disease objects
            'WBVar:WBVar00000012', 'WBVar:WBVar00000013', 'WB:WBVar00000001', 'WB:WBVar00242490'
        }

        self.sgdIdSet = {
            'SGD:S000003256', 'SGD:S000003513', 'SGD:S000000119', 'SGD:S000001015',
            # phenotypic genes
            'SGD:S000001101', 'SGD:S000006136', 'SGD:S000000383',
            # expression
            'SGD:S000005737', 'SGD:S000004802', 'SGD:S000000002'
        }

        self.zfinIdSet = {
            'ZFIN:ZDB-GENE-990415-72', 'ZFIN:ZDB-GENE-030131-3445', 'ZFIN:ZDB-GENE-980526-388',
            'ZFIN:ZDB-GENE-010525-1', 'ZFIN:ZDB-GENE-010525-1', 'ZFIN:ZDB-GENE-060117-5',
            'ZFIN:ZDB-GENE-050302-80', 'ZFIN:ZDB-GENE-060503-876',
            'ZFIN:ZDB-GENE-050302-82', 'ZFIN:ZDB-GENE-030131-4430', 'ZFIN:ZDB-GENE-060503-872',
            'ZFIN:ZDB-GENE-060503-873', 'ZFIN:ZDB-GENE-010525-1', 'ZFIN:ZDB-GENE-990415-72',
            'ZFIN:ZDB-GENE-060503-867', 'ZFIN:ZDB-GENE-010323-11', 'ZFIN:ZDB-GENE-010525-1',
            'ZFIN:ZDB-GENE-010320-1', 'ZFIN:ZDB-GENE-010525-1', 'ZFIN:ZDB-GENE-051127-5',
            'ZFIN:ZDB-GENE-990415-270', 'ZFIN:ZDB-LINCRNAG-160518-1', 'ZFIN:ZDB-GENE-030131-3776', 'ZFIN:ZDB-GENE-030616-47',
            'ZFIN:ZDB-GENE-040426-1716', 'ZFIN:ZDB-ALT-980203-985', 'ZFIN:ZDB-ALT-060608-195',
            'ZFIN:ZDB-ALT-050428-6', 'ZFIN:ZDB-ALT-151012-9',
            # disease specific test objects
            'ZFIN:ZDB-GENE-030131-5607', 'ZFIN:ZDB-GENE-050517-20', 'ZFIN:ZDB-GENE-990415-122',
            'ZFIN:ZDB-GENE-050517-20', 'ZFIN:ZDB-ALT-980203-1091',
            'ZFIN:ZDB-GENE-000816-1', 'ZFIN:ZDB-ALT-160129-6', 'ZFIN:ZDB-ALT-160129-6',
            # expression
            'ZFIN:ZDB-GENE-070410-17', 'ZFIN:ZDB-GENE-990714-29', 'ZFIN:ZDB-GENE-001103-1', 'ZFIN:ZDB-GENE-050913-20',
            'ZFIN:ZDB-GENE-980526-474', 'ZFIN:ZDB-GENE-000627-1', 'ZFIN:ZDB-GENE-050913-20', 'ZFIN:ZDB-GENE-030912-6',
            'ZFIN:ZDB-GENE-131121-260', 'ZFIN:ZDB-GENE-980526-368'
        }
        self.flybaseIdSet = {
            'FB:FBgn0083973', 'FB:FBgn0037960', 'FB:FBgn0027296', 'FB:FBgn0032006', 'FB:FBgn0001319',
            'FB:FBgn0002369', 'FB:FBgn0026379',
            'FB:FBgn0033885', 'FB:FBgn0024320', 'FB:FBgn0283499',
            'FB:FBgn0032465', 'FB:FBgn0285944', 'FB:FBgn0032728', 'FB:FBgn0000014',
            'FB:FBgn0032729', 'FB:FBgn0065610', 'FB:FBgn0032730', 'FB:FBgn0032732', 'FB:FBgn0260987',
            'FB:FBgn0032781', 'FB:FBgn0032782', 'FB:FBgn0032740',
            'FB:FBgn0032741', 'FB:FBgn0032744', 'FB:FBgn0036309', 'FB:FBgn0003470', 'FB:FBal0161187', 'FB:FBal0000003',
            'FB:FBal0000004', 'FB:FBgn0039156',
            # expression
            'FB:FBgn0027660', 'FB:FBgn0284221', 'FB:FBgn0013765', 'FB:FBgn0004620'

        }
        self.rgdTestSet = {
            'RGD:70891', 'RGD:1306349', 'RGD:708528', 'RGD:620796', 'RGD:61995', 'RGD:1309165',
            'RGD:1581495', 'RGD:2322065', 'RGD:1309063', 'RGD:2845', 'RGD:628748', 'RGD:1581476',
            'RGD:1309312', 'RGD:7627512', 'RGD:1309105', 'RGD:1309109', 'RGD:7627503', 'RGD:1578801',
            # disease pheno specific test objects
            'RGD:68936', 'RGD:3886',
            # expression
            'RGD:3884', 'RGD:3889'
        }

        self.humanTestSet = {
            'HGNC:17889', 'HGNC:25818', 'HGNC:3686', 'HGNC:7881', 'HGNC:6709', 'HGNC:6526', 'HGNC:6553', 'HGNC:7218',
            'HGNC:6560', 'HGNC:6551', 'HGNC:6700', 'HGNC:9588', 'HGNC:11973',
            # disease pheno specific test objects
            'HGNC:897', 'HGNC:869'
        }

        self.testOntologyTerms = {'DOID:0110741', 'DOID:0110739', 'DOID:10021', 'DOID:10030', 'DOID:0001816',
                             'DOID:0060171', 'DOID:1115', 'DOID:0001816', 'DOID:14330', 'DOID:9452',
                             'DOID:9455', 'DOID:1059', 'DOID:9409',
                             'GO:0019899', 'GO:0005515', 'GO:0043393', 'GO:0022607',
                             'GO:0009952', 'GO:0005764', 'GO:0060271', 'GO:0048263',
                             'GO:0007492', 'GO:0030902', 'GO:0070121', 'GO:0030901', 'GO:0030182',
                             'GO:0042664', 'GO:0030916', 'GO:0021571', 'GO:0061195', 'GO:0048705', 'GO:0030335',
                             'GO:0048709'}

        self.modMap = {"RGD": self.rgdTestSet,
                  "MGI": self.mgiIdSet,
                  "ZFIN": self.zfinIdSet,
                  "WB": self.wormbaseIdSet,
                  "SGD": self.sgdIdSet,
                  "FlyBase": self.flybaseIdSet,
                  "Human": self.humanTestSet}

        #TODO use method below, or add more mods here as they become available. add back in RGD human
        self.testIdSet = self.zfinIdSet.union(self.mgiIdSet.union(self.wormbaseIdSet).union(self.flybaseIdSet).union(self.sgdIdSet).union(self.rgdTestSet).union(self.humanTestSet))
        #self.testIdSet = self.mgiIdSet

    def assemble_test_data(modList, modMap):
        testIdSet = {}
        for aggregateLoaderMOD in modList:
            logger.info (aggregateLoaderMOD)
            for modToTest, modTestIdSet in modMap.items():
                logger.info (modToTest)
                if aggregateLoaderMOD == modToTest:
                    testIdSet.union(modTestIdSet)
        return testIdSet

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
