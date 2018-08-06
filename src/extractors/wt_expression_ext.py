import uuid
from services import DataProvider
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor


class WTExpressionExt(object):

    def get_wt_expression_data(self, wt_expression_data, batch_size, testObject, species):
        list_to_yield = []
        xrefUrlMap = ResourceDescriptor().get_data()
        dateProduced = wt_expression_data['metaData']['dateProduced']
        dataProviders = []
        loadKey = ""
        crossReferences = []
        for dataProviderObject in wt_expression_data['metaData']['dataProvider']:

            dataProviderCrossRef = dataProviderObject.get('crossReference')
            dataProviderType = dataProviderObject.get('type')
            dataProvider = dataProviderCrossRef.get('id')
            dataProviderPages = dataProviderCrossRef.get('pages')
            dataProviderCrossRefSet = []
            dataProviders = []
            loadKey = loadKey + dateProduced + dataProvider + "_BGI"

            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
                                                                       dataProviderPage)
                dataProviderCrossRefSet.append(
                    CreateCrossReference.get_xref(dataProvider, dataProvider, dataProviderPage,
                                                  dataProviderPage, dataProvider, crossRefCompleteUrl,
                                                  dataProvider + dataProviderPage))

            dataProviders.append(dataProvider)

        dataProviderSingle = DataProvider().get_data_provider(species)
        print ("dataProvider found: " + dataProviderSingle)

        for xpat in wt_expression_data['data']:

            pubMedUrl = None
            pubModUrl = None
            geneId = xpat.get('geneId')
            whenExpressedStage = xpat.get('whenExpressedStage')

            if testObject.using_test_data() is True:
                is_it_test_entry = testObject.check_for_test_id_entry(primaryId)
                if is_it_test_entry is False:
                    continue

            if 'evidence' in xpat:

                pubModId = ""
                pubMedId = ""
                pubModUrl = None
                pubMedUrl = None

                evidence = xpat.get('evidence')
                if 'publication' in evidence:
                    if 'modPublicationId' in evidence['publication']:
                        publicationModId = evidence['publication'].get('modPublicationId')
                        pubModLocalId = publicationModId.split(":")[1]
                        pubModPrefix = publicationModId.split(":")[0]
                        pubModUrl = UrlService.get_page_complete_url(pubModLocalId, xrefUrlMap, pubModPrefix, "gene/references")
                    if 'pubMedId' in evidence['publication']:
                        pubMedId = evidence['publication'].get('pubMedId')
                        localPubMedId = pubMedId.split(":")[1]
                        pubMedPrefix = pubMedId.split(":")[0]
                        pubMedUrl = UrlService.get_no_page_complete_url(localPubMedId, xrefUrlMap, pubMedPrefix, primaryId)

            if pubMedId == None:
                pubMedId = ""

            if pubModId == None:
                pubModId = ""

            dateAssigned = xpat.get('dateAssigned')

            if pubModId == None and pubMedId == None:
                print (geneId + "is missing pubMed and pubMod id")

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
            if 'wildtypeExpressionTermIdentifiers' in xpat:

                wildtypeExpressionTermIdentifers = xpat.get('wildtypeExpressionTermIdentifiers')
                cellularComponentQualifierTermId = wildtypeExpressionTermIdentifers.get('cellularComponentQualifierTermId')
                cellularComponentTermId =  wildtypeExpressionTermIdentifers.get('cellularComponentTermId')
                print ("first cellular component: " + cellularComponentTermId)
                anatomicalStructureTermId = wildtypeExpressionTermIdentifers.get('anatomicalStructureTermId')
                anatomicalStructureQualifierTermId = wildtypeExpressionTermIdentifers.get('acnatomicalStructureQualifierTermId')
                anatomicalSubStructureTermId = wildtypeExpressionTermIdentifers.get('anatomicalSubStructureTermId')
                anatomicalSubStructureQualifierTermId = wildtypeExpressionTermIdentifers.get('anatomicalSubStructureQualifierTermId')
                whereExpressedStatement = wildtypeExpressionTermIdentifers.get('whereExpressedStatement')

                if cellularComponentQualifierTermId == None:
                    cellularComponentQualifierTermId = ""
                if cellularComponentTermId== None:
                    cellularComponentTermId = ""
                if  anatomicalStructureTermId== None:
                    anatomicalStructureTermId = ""
                if anatomicalStructureQualifierTermId == None:
                    anatomicalStructureQualifierTermId = ""
                if anatomicalSubStructureTermId == None:
                    anatomicalSubStructureTermId  = ""
                if anatomicalSubStructureQualifierTermId == None:
                    anatomicalSubStructureQualifierTermId = ""
                if whereExpressedStatement == None:
                    whereExpressedStatement = ""

                print ("second cellular component: " + cellularComponentTermId)

            assay = xpat.get('assay')
            print ("assay: " + assay)

            expression = {
                "geneId": geneId,
                "whenExpressedStage": whenExpressedStage,
                "dateAssigned": dateAssigned,
                "pubMedId": pubMedId,
                "pubMedUrl": pubMedUrl,
                "pubModId": pubModId,
                "pubModUrl": pubModUrl,
                "pubPrimaryKey": pubMedId + pubModId,
                "uuid": str(uuid.uuid4()),
                "loadKey": loadKey,
                "type": "gene",
                "dataProviders": dataProviders,
                "dataProviderType": dataProviderType,
                "dateProduced": dateProduced,
                "dataProvider": dataProviderSingle,
                "assay": assay,
                "crossReferences": crossReferences,
                "cellularComponentQualifierTermId": cellularComponentQualifierTermId,
                "cellularComponentTermId": cellularComponentTermId,
                "anatomicalStructureTermId": anatomicalStructureTermId,
                "anatomicalStructureQualifierTermId": anatomicalStructureQualifierTermId,
                "anatomicalSubStructureTermId": anatomicalSubStructureTermId,
                "anatomicalSubStructureQualifierTermId": anatomicalSubStructureQualifierTermId,
                "whereExpressedStatement": whereExpressedStatement,
                "expressionEntityUuid": str(uuid.uuid4()),
                "expressionEntityPk": cellularComponentTermId+cellularComponentQualifierTermId+anatomicalStructureTermId+anatomicalStructureQualifierTermId+anatomicalSubStructureTermId+anatomicalSubStructureQualifierTermId,
                "pubPrimaryKey": pubMedId + pubModId,
                "uuidGeneExpressionJoin": str(uuid.uuid4()),
                "uuidCCJoin": str(uuid.uuid4()),
                "uuidASSJoin": str(uuid.uuid4()),
                "uuidASJoin": str(uuid.uuid4())
             }

            list_to_yield.append(expression)

            if len(list_to_yield) == batch_size:
                yield list_to_yield
                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield