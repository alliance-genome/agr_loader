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
        logger.info ("loadFile: " + loadFile)
        batch_size = 10000
        list_to_yield = []
        xrefUrlMap = ResourceDescriptor().get_data()

        dataProviders = []
        loadKey = ""
        crossReferences = []

        logger.info("streaming json data from %s ..." % loadFile)
        with codecs.open(loadFile, 'r', 'utf-8') as f:
            logger.info("file open")
            for xpat in ijson.items(f, 'data.item'):
                pubMedUrl = None
                pubModUrl = None
                pubMedId = ""
                publicationModId = ""
                stageTermId = ""
                stageName = ""
                stageUberonTermId = ""
                aoStructureUberonTerms = []
                aoSubStructureUberonTerms = []
                geneId = xpat.get('geneId')

                if testObject.using_test_data() is True:
                    is_it_test_entry = testObject.check_for_test_id_entry(geneId)
                    if is_it_test_entry is False:
                        continue

                whenExpressedStage = xpat.get('whenExpressed')
                if 'stageTermId' in whenExpressedStage:
                    stageTermId = whenExpressedStage.get('stageTermId')
                if 'stageName' in whenExpressedStage:
                    stageName = whenExpressedStage.get('stageName')
                if 'stageUberonSlimTerm' in whenExpressedStage:
                    stageUberonTermObject = whenExpressedStage.get('stageUberonSlimTerm')
                    stageUberonTermId = stageUberonTermObject.get("uberonTerm")

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

                #dateAssigned = xpat.get('dateAssigned')

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

                if 'whereExpressed' in xpat:

                    whereExpressedStatement = xpat.get('whereExpressed')
                    cellularComponentQualifierTermId = whereExpressedStatement.get('cellularComponentQualifierTermId')
                    cellularComponentTermId = whereExpressedStatement.get('cellularComponentTermId')
                    anatomicalStructureTermId = whereExpressedStatement.get('anatomicalStructureTermId')
                    anatomicalStructureQualifierTermId = whereExpressedStatement.get(
                        'anatomicalStructureQualifierTermId')
                    anatomicalSubStructureTermId = whereExpressedStatement.get('anatomicalSubStructureTermId')
                    anatomicalSubStructureQualifierTermId = whereExpressedStatement.get(
                        'anatomicalSubStructureQualifierTermId')
                    whereExpressedStatement = whereExpressedStatement.get('whereExpressedStatement')

                    if 'anatomicalStructureUberonSlimTermIds' in whereExpressedStatement:
                        for uberonStructureTermObject in whereExpressedStatement.get('anatomicalStructureUberonSlimTermIds'):
                            structureUberonTermId = uberonStructureTermObject.get('uberonTerm')
                            aoStructureUberonTerms.append(structureUberonTermId)

                    if 'anatomicalSubStructureUberonSlimTermIds' in whereExpressedStatement:
                        for uberonSubStructureTermObject in whereExpressedStatement.get('anatomicalSubStructureUberonSlimTermIds'):
                            subStructureUberonTermId = uberonSubStructureTermObject.get('uberonTerm')
                            aoSubStructureUberonTerms.append(subStructureUberonTermId)

                    if cellularComponentQualifierTermId is None:
                        cellularComponentQualifierTermId = ""
                    if cellularComponentTermId is None:
                        cellularComponentTermId = ""
                    if anatomicalStructureTermId is None:
                        anatomicalStructureTermId = ""
                    if anatomicalStructureQualifierTermId is None:
                        anatomicalStructureQualifierTermId = ""
                    if anatomicalSubStructureTermId is None:
                        anatomicalSubStructureTermId = ""
                    if anatomicalSubStructureQualifierTermId is None:
                        anatomicalSubStructureQualifierTermId = ""
                    if whereExpressedStatement is None:
                        whereExpressedStatement = ""

                    assay = xpat.get('assay')

                    expression = {
                        "geneId": geneId,
                        "whenExpressedStage": whenExpressedStage,
                        # "dateAssigned": dateAssigned,
                        "pubMedId": pubMedId,
                        "pubMedUrl": pubMedUrl,
                        "pubModId": publicationModId,
                        "pubModUrl": pubModUrl,
                        "pubPrimaryKey": pubMedId + publicationModId,
                        "uuid": str(uuid.uuid4()),
                        "loadKey": loadKey,
                        "type": "gene",
                        "aoStructureUberonTerms": aoStructureUberonTerms,
                        "aoSubStructureUberonTerms": aoSubStructureUberonTerms,
                        "stageTermId": stageTermId,
                        "stageName": stageName,
                        "stageUberonTermId": stageUberonTermId,
                        "dataProviders": dataProviders,
                        # "dataProviderType": dataProviderType,
                        # "dateProduced": dateProduced,
                        # "dataProvider": dataProviderSingle,
                        "assay": assay,
                        "crossReferences": crossReferences,
                        "cellularComponentQualifierTermId": cellularComponentQualifierTermId,
                        "cellularComponentTermId": cellularComponentTermId,
                        "anatomicalStructureTermId": anatomicalStructureTermId,
                        "anatomicalStructureQualifierTermId": anatomicalStructureQualifierTermId,
                        "anatomicalSubStructureTermId": anatomicalSubStructureTermId,
                        "anatomicalSubStructureQualifierTermId": anatomicalSubStructureQualifierTermId,
                        "whereExpressedStatement": whereExpressedStatement,
                        "expressionEntityPk": cellularComponentTermId + cellularComponentQualifierTermId + anatomicalStructureTermId + anatomicalStructureQualifierTermId + anatomicalSubStructureTermId + anatomicalSubStructureQualifierTermId,
                        "pubPrimaryKey": pubMedId + publicationModId,
                        "ei_uuid": str(uuid.uuid4()),
                        "s_uuid": str(uuid.uuid4()),
                        "ss_uuid": str(uuid.uuid4()),
                        "cc_uuid": str(uuid.uuid4()),
                        "ebe_uuid": str(uuid.uuid4())
                    }

                    list_to_yield.append(expression)
                    if len(list_to_yield) == batch_size:
                        yield list_to_yield
                        list_to_yield[:] = []  # Empty the list.

            if len(list_to_yield) > 0:
                logger.info(geneId)
                yield list_to_yield

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
