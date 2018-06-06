import uuid
from services import UrlService
from services import CreateCrossReference
from services import DataProvider
from .resource_descriptor_ext import ResourceDescriptor


class AlleleExt(object):
    def get_alleles(self, allele_data, batch_size, testObject, species):

        xrefUrlMap = ResourceDescriptor().get_data()
        dataProviders = []
        list_to_yield = []
        loadKey = ""

        dateProduced = allele_data['metaData']['dateProduced']


        for dataProviderObject in allele_data['metaData']['dataProvider']:

            dataProviderCrossRef = dataProviderObject.get('crossReference')
            dataProvider = dataProviderCrossRef.get('id')
            print (dataProvider + "allele")
            dataProviderPages = dataProviderCrossRef.get('pages')
            dataProviderCrossRefSet = []
            release = None
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

        if 'release' in allele_data['metaData']:
            release = allele_data['metaData']['release']

        for alleleRecord in allele_data['data']:
            crossReferences = []
            globalId = alleleRecord['primaryId']
            localId = globalId.split(":")[1]
            modGlobalCrossRefId = ""

            if testObject.using_test_data() is True:
                is_it_test_entry = testObject.check_for_test_id_entry(globalId)
                if is_it_test_entry is False:
                    continue

            if 'crossReferences' in alleleRecord:

                for crossRef in alleleRecord['crossReferences']:
                    crossRefId = crossRef.get('id')
                    local_crossref_id = crossRefId.split(":")[1]
                    prefix = crossRef.get('id').split(":")[0]
                    pages = crossRef.get('pages')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            if page == 'allele':
                                modGlobalCrossRefId = UrlService.get_page_complete_url(local_crossref_id, xrefUrlMap, prefix, page)
                                crossReferences.append(
                                    CreateCrossReference.get_xref(local_crossref_id, prefix, page, page, crossRefId, modGlobalCrossRefId, crossRefId+page))
            allele_dataset = {
                "symbol": alleleRecord.get('symbol'),
                "geneId": alleleRecord.get('gene'),
                "primaryId": alleleRecord.get('primaryId'),
                "globalId": globalId,
                "localId": localId,
                "taxonId": alleleRecord.get('taxonId'),
                "synonyms": alleleRecord.get('synonyms'),
                "secondaryIds": alleleRecord.get('secondaryIds'),
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "loadKey": loadKey,
                "release": release,
                "modGlobalCrossRefId": modGlobalCrossRefId,
                "uuid": str(uuid.uuid4()),
                "crossReferences": crossReferences
            }

            list_to_yield.append(allele_dataset)
            if len(list_to_yield) == batch_size:
                yield list_to_yield

                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield
