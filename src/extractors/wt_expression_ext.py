import uuid
import ijson
import codecs
import pprint
from files import S3File, TARFile, JSONFile
from services import DataProvider
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class WTExpressionExt(object):

    def get_wt_expression_data(self, loadFile, expressionFile, batch_size, testObject):
        path = "tmp"
        S3File(loadFile, path).download()
        TARFile(path, loadFile).extract_all()
        loadFile = path + expressionFile
        logger.info("loadFile: " + loadFile)
        batch_size = 10000
        xrefUrlMap = ResourceDescriptor().get_data()

        crossReferences = []
        counter = 0
        aoExpression = []
        ccExpression = []
        aoQualifier = []
        aoSubstructure = []
        aoSSQualifier = []
        ccQualifier = []
        aoccExpression = []
        stageList = []
        stageUberonData = []
        uberonAOData =[]
        uberonAOOtherData = []
        uberonStageOtherData = []

        logger.info("streaming json data from %s ..." % loadFile)
        with codecs.open(loadFile, 'r', 'utf-8') as f:
            logger.info("file open")
            for xpat in ijson.items(f, 'data.item'):
                counter = counter + 1

                pubMedUrl = None
                pubModUrl = None
                pubMedId = ""
                publicationModId = ""
                stageTermId = ""
                stageName = ""
                stageUberonTermId = ""
                geneId = xpat.get('geneId')

                if testObject.using_test_data() is True:
                    is_it_test_entry = testObject.check_for_test_id_entry(geneId)
                    if is_it_test_entry is False:
                        continue




                evidence = xpat.get('evidence')

                if 'modPublicationId' in evidence:
                    publicationModId = evidence.get('modPublicationId')
                    pubModLocalId = publicationModId.split(":")[1]
                    pubModPrefix = publicationModId.split(":")[0]
                    pubModUrl = UrlService.get_page_complete_url(pubModLocalId, xrefUrlMap, pubModPrefix, "gene/references")
                    if publicationModId is None:
                        publicationModId =""

                if 'pubMedId' in evidence:
                    pubMedId = evidence.get('pubMedId')
                    localPubMedId = pubMedId.split(":")[1]
                    pubMedPrefix = pubMedId.split(":")[0]
                    pubMedUrl = UrlService.get_no_page_complete_url(localPubMedId, xrefUrlMap, pubMedPrefix, geneId)
                    if pubMedId is None:
                        pubMedId = ""

                if 'crossReference' in xpat:
                    crossRef = xpat.get('crossReference')
                    crossRefId = crossRef.get('id')
                    local_crossref_id = crossRefId.split(":")[1]
                    prefix = crossRef.get('id').split(":")[0]
                    pages = crossRef.get('pages')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            if page == 'allele':
                                modGlobalCrossRefId = UrlService.get_page_complete_url(local_crossref_id, xrefUrlMap,
                                                                               prefix, page)
                                crossReferences.append(
                                CreateCrossReference.get_xref(local_crossref_id, prefix, page, page, crossRefId,
                                                          modGlobalCrossRefId, crossRefId + page))
                assay = xpat.get('assay')
                ei_uuid = str(uuid.uuid4())
                ebe_uuid = str(uuid.uuid4())

                whenExpressedStage = xpat.get('whenExpressed')

                if 'stageTermId' in whenExpressedStage:
                    stageTermId = whenExpressedStage.get('stageTermId')
                if 'stageName' in whenExpressedStage:
                    stageName = whenExpressedStage.get('stageName')
                if 'stageUberonSlimTerm' in whenExpressedStage:
                    stageUberonTermObject = whenExpressedStage.get('stageUberonSlimTerm')
                    stageUberonTermId = stageUberonTermObject.get("uberonTerm")
                    if stageUberonTermId is not None and stageUberonTermId != "post embryonic, pre-adult":
                        stageUberon = {
                            "uberonStageId": stageUberonTermId,
                            "ei_uuid": ei_uuid
                        }
                        stageUberonData.append(stageUberon)
                    if stageUberonTermId == "post embryonic, pre-adult":
                        stageUberonOther = {
                            "ei_uuid": ei_uuid
                        }
                        uberonStageOtherData.append(stageUberonOther)

                if stageTermId is None or stageName == 'N/A':
                    stageTermId = ""
                    stageName = ""
                    stageUberonTermId = ""

                if stageName is not None:

                    stage = {
                        "stageTermId": stageTermId,
                        "stageName": stageName,
                        "ei_uuid": ei_uuid
                    }

                    stageList.append(stage)

                else:
                    stageUberonTermId = ""

                if 'whereExpressed' in xpat:

                    whereExpressed = xpat.get('whereExpressed')
                    cellularComponentQualifierTermId = whereExpressed.get('cellularComponentQualifierTermId')
                    cellularComponentTermId = whereExpressed.get('cellularComponentTermId')
                    anatomicalStructureTermId = whereExpressed.get('anatomicalStructureTermId')
                    anatomicalStructureQualifierTermId = whereExpressed.get(
                        'anatomicalStructureQualifierTermId')
                    anatomicalSubStructureTermId = whereExpressed.get('anatomicalSubStructureTermId')
                    anatomicalSubStructureQualifierTermId = whereExpressed.get(
                        'anatomicalSubStructureQualifierTermId')
                    whereExpressedStatement = whereExpressed.get('whereExpressedStatement')

                    if 'anatomicalStructureUberonSlimTermIds' in whereExpressed:
                        for uberonStructureTermObject in whereExpressed.get('anatomcialStructureUberonSlimTermIds'):
                            structureUberonTermId = uberonStructureTermObject.get('uberonTerm')
                            logger.info("structureUberonTermId:" + structureUberonTermId)
                            if structureUberonTermId is not None and structureUberonTermId != 'Other':
                                structureUberonTerm = {
                                    "ebe_uuid": ebe_uuid,
                                    "aoUberonId": structureUberonTermId
                                }
                                uberonAOData.append(structureUberonTerm)
                                logger.info("structureUberonTermId:" + structureUberonTermId)
                            elif structureUberonTermId is not None and structureUberonTermId == 'Other':
                                otherStructureUberonTerm = {
                                    "ebe_uuid": ebe_uuid
                                }
                                uberonAOOtherData.append(otherStructureUberonTerm)

                    if 'anatomicalSubStructureUberonSlimTermIds' in whereExpressed:
                        for uberonSubStructureTermObject in whereExpressed.get('anatomicalSubStructureUberonSlimTermIds'):
                            subStructureUberonTermId = uberonSubStructureTermObject.get('uberonTerm')
                            if subStructureUberonTermId is not None and subStructureUberonTermId != 'Other':
                                subStructureUberonTerm = {
                                    "ebe_uuid": ebe_uuid,
                                    "aoUberonId": subStructureUberonTermId
                                }
                                uberonAOData.append(subStructureUberonTerm)
                            elif subStructureUberonTermId is not None and subStructureUberonTermId == 'Other':
                                otherStructureUberonTerm = {
                                    "ebe_uuid": ebe_uuid
                                }
                                uberonAOOtherData.append(otherStructureUberonTerm)

                    if cellularComponentTermId is None:
                        cellularComponentTermId = ""

                        AOExpression = {
                            "geneId": geneId,
                            "whenExpressedStage": whenExpressedStage,
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": publicationModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "uuid": str(uuid.uuid4()),
                            "assay": assay,
                            "crossReferences": crossReferences,
                            "anatomicalStructureTermId": anatomicalStructureTermId,
                            "whereExpressedStatement": whereExpressedStatement,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "ei_uuid": ei_uuid,
                            "ebe_uuid": ebe_uuid

                        }
                        aoExpression.append(AOExpression)

                    if cellularComponentQualifierTermId is not None:

                        CCQualifier = {
                            "ebe_uuid": ebe_uuid,
                            "cellularComponentQualifierTermId": cellularComponentQualifierTermId

                        }
                        ccQualifier.append(CCQualifier)

                    if anatomicalStructureTermId is None:
                        anatomicalStructureTermId = ""

                        CCExpression = {
                            "geneId": geneId,
                            "whenExpressedStage": whenExpressedStage,
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": publicationModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "assay": assay,
                            "crossReferences": crossReferences,
                            "whereExpressedStatement": whereExpressedStatement,
                            "cellularComponentTermId": cellularComponentTermId,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "ei_uuid": ei_uuid,
                            "ebe_uuid": ebe_uuid
                        }
                        ccExpression.append(CCExpression)

                    if anatomicalStructureQualifierTermId is not None:
                        AOQualifier = {
                            "ebe_uuid": ebe_uuid,
                            "anatomicalStructureQualifierTermId": anatomicalStructureQualifierTermId
                        }
                        aoQualifier.append(AOQualifier)

                    if anatomicalSubStructureTermId is not None:
                        AOSubstructure = {
                            "ebe_uuid": ebe_uuid,
                            "anatomicalSubStructureTermId": anatomicalSubStructureTermId

                        }
                        aoSubstructure.append(AOSubstructure)

                    if anatomicalSubStructureQualifierTermId is not None:
                        AOSSQualifier = {
                            "ebe_uuid": ebe_uuid,
                            "anatomicalSubStructureQualifierTermId": anatomicalSubStructureQualifierTermId

                        }
                        aoSSQualifier.append(AOSSQualifier)

                    if whereExpressedStatement is None:
                        whereExpressedStatement = ""

                    if anatomicalStructureTermId is not None and anatomicalStructureTermId != "" and cellularComponentTermId is not None and cellularComponentTermId != "":

                        AOCCExpression = {
                            "geneId": geneId,
                            "whenExpressedStage": whenExpressedStage,
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": publicationModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "uuid": str(uuid.uuid4()),
                            "stageTermId": stageTermId,
                            "stageName": stageName,
                            "stageUberonTermId": stageUberonTermId,
                            "assay": assay,
                            "crossReferences": crossReferences,
                            "cellularComponentTermId": cellularComponentTermId,
                            "anatomicalStructureTermId": anatomicalStructureTermId,
                            "whereExpressedStatement": whereExpressedStatement,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "ei_uuid": ei_uuid,
                            "ebe_uuid": ebe_uuid
                        }

                        aoccExpression.append(AOCCExpression)

                    if counter == batch_size:
                        yield (aoExpression, ccExpression, aoQualifier, aoSubstructure, aoSSQualifier, ccQualifier, aoccExpression, stageList, stageUberonData, uberonAOData, uberonAOOtherData, uberonStageOtherData)
                        aoExpression = []
                        ccExpression = []
                        aoQualifier = []
                        aoSubstructure = []
                        aoSSQualifier = []
                        ccQualifier = []
                        aoccExpression = []
                        stageList = []
                        uberonStageOtherData = []
                        stageUberonData = []
                        uberonAOOtherData = []
                        uberonAOData = []
                        counter = 0

            if counter > 0:
                logger.info(geneId)
                yield (aoExpression, ccExpression, aoQualifier, aoSubstructure, aoSSQualifier, ccQualifier, aoccExpression, stageList, stageUberonData, uberonAOData, uberonAOOtherData, uberonStageOtherData)

        f.close()
        # TODO: get dataProvider parsing working with ijson.
        # wt_expression_data = JSONFile().get_data(path + expressionName, 'expression')
        #
        # dateProduced = wt_expression_data['metaData']['dateProduced']
        # for dataProviderObject in wt_expression_data['metaData']['dataProvider']:
        #
        #     dataProviderCrossRef = dataProviderObject.get('crossReference')
        #     dataProviderType = dataProviderObject.get('type')
        #     dataProvider = dataProviderCrossRef.get('id')
        #     dataProviderPages = dataProviderCrossRef.get('pages')
        #     dataProviderCrossRefSet = []
        #     dataProviders = []
        #     loadKey = loadKey + dateProduced + dataProvider + "_BGI"
        #
        #     for dataProviderPage in dataProviderPages:
        #         crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
        #                                                                dataProviderPage)
        #         dataProviderCrossRefSet.append(
        #             CreateCrossReference.get_xref(dataProvider, dataProvider, dataProviderPage,
        #                                           dataProviderPage, dataProvider, crossRefCompleteUrl,
        #                                           dataProvider + dataProviderPage))
        #
        #     dataProviders.append(dataProvider)
        #
        # dataProviderSingle = DataProvider().get_data_provider(species)
        # logger.info ("dataProvider found: " + dataProviderSingle)
