import uuid
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor
from services import DataProvider


class PhenotypeExt(object):

    def get_phenotype_data(self, phenotype_data, batch_size, testObject, species):
        list_to_yield = []
        xrefUrlMap = ResourceDescriptor().get_data()
        dateProduced = phenotype_data['metaData']['dateProduced']
        dataProviders = []
        loadKey = ""

        for dataProviderObject in phenotype_data['metaData']['dataProvider']:

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

        for pheno in phenotype_data['data']:

            pubMedUrl = None
            pubModUrl = None
            primaryId = pheno.get('objectId')
            phenotypeStatement = pheno.get('phenotypeStatement')

            if testObject.using_test_data() is True:
                is_it_test_entry = testObject.check_for_test_id_entry(primaryId)
                if is_it_test_entry is False:
                    continue

            if dataProviderSingle != 'MGI':

                evidence = pheno.get('evidence')
                if 'publication' in evidence:
                    if 'modPublicationId' in evidence['publication']:
                        pubModId = evidence['publication'].get('modPublicationId')

                    if 'pubMedId' in evidence['publication']:
                        pubMedId = evidence['publication'].get('pubMedId')
            else:
                pubMedId = pheno.get('pubMedId')
                pubModId = pheno.get('pubModId')

            if pubMedId != None:
                pubMedPrefix = pubMedId.split(":")[0]
                pubMedLocalId = pubMedId.split(":")[1]
                pubMedUrl = UrlService.get_no_page_complete_url(pubMedLocalId, xrefUrlMap, pubMedPrefix, primaryId)

                pubModId = pheno.get('pubModId')

            if pubModId != None:
                pubModPrefix = pubModId.split(":")[0]
                pubModLocalId = pubModId.split(":")[1]
                pubModUrl = UrlService.get_page_complete_url(pubModLocalId, xrefUrlMap, pubModPrefix, "gene/references")

            if pubMedId == None:
                pubMedId = ""

            if pubModId == None:
                pubModId = ""

            dateAssigned = pheno.get('dateAssigned')

            if pubModId == None and pubMedId == None:
                print (primaryId + "is missing pubMed and pubMod id")

            phenotype_feature = {
                "primaryId": primaryId,
                "phenotypeStatement": phenotypeStatement,
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
                "dataProvider": dataProviderSingle
             }

            list_to_yield.append(phenotype_feature)

            if len(list_to_yield) == batch_size:
                yield list_to_yield
                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield