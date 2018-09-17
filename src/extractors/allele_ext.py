import uuid
from services import UrlService
from services import CreateCrossReference
from services import DataProvider
from .resource_descriptor_ext import ResourceDescriptor
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class AlleleExt(object):
    def get_alleles(self, allele_data, batch_size, testObject, species):

        xrefUrlMap = ResourceDescriptor().get_data()
        dataProviders = []
        release = ""
        alleles = []
        allele_synonyms = []
        allele_secondaryIds = []
        crossReferences = []

        counter = 0
        dateProduced = allele_data['metaData']['dateProduced']

        dataProviderObject = allele_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        loadKey = dateProduced + dataProvider + "_BGI"

        #TODO: get SGD to fix their files.

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
                                                                       dataProviderPage)

                dataProviderCrossRefSet.append(CreateCrossReference.get_xref(dataProvider, dataProvider,
                                                                             dataProviderPage,
                                                                             dataProviderPage, dataProvider,
                                                                             crossRefCompleteUrl,
                                                                             dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.info("data provider: " + dataProvider)

        dataProviderSingle = DataProvider().get_data_provider(species)

        if 'release' in allele_data['metaData']:
            release = allele_data['metaData']['release']

        for alleleRecord in allele_data['data']:
            counter = counter + 1
            crossReferences = []
            globalId = alleleRecord['primaryId']
            localId = globalId.split(":")[1]
            modGlobalCrossRefId = ""

            if testObject.using_test_data() is True:
                is_it_test_entry = testObject.check_for_test_id_entry(globalId)
                if is_it_test_entry is False:
                    counter = counter - 1
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
                                xref = CreateCrossReference.get_xref(local_crossref_id, prefix, page, page, crossRefId, modGlobalCrossRefId, crossRefId+page)
                                xref['dataId'] = globalId
                                logger.info(xref)
                                crossReferences.append(xref)

            allele_dataset = {
                "symbol": alleleRecord.get('symbol'),
                "geneId": alleleRecord.get('gene'),
                "primaryId": alleleRecord.get('primaryId'),
                "globalId": globalId,
                "localId": localId,
                "taxonId": alleleRecord.get('taxonId'),
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "loadKey": loadKey,
                "release": release,
                "modGlobalCrossRefId": modGlobalCrossRefId,
                "uuid": str(uuid.uuid4()),
                "dataProvider": dataProviderSingle,
                "symbolText": alleleRecord.get('symbolText')
            }

            if allele_dataset.get('synonyms') is not None:
                for synonym in allele_dataset.get('synonyms'):
                    allele_synonym = {
                        "data_id": alleleRecord.get('primaryId'),
                        "synonym": synonym
                    }
                    allele_synonyms.append(allele_synonym)

            if allele_dataset.get('secondaryIds') is not None:
                for secondaryId in allele_dataset.get('secondaryIds'):
                    allele_secondaryId = {
                        "data_id": alleleRecord.get('primaryId'),
                        "secondary_id": secondaryId
                    }
                    allele_secondaryIds.append(allele_secondaryId)

            alleles.append(allele_dataset)
            if counter == batch_size:
                yield (alleles, allele_secondaryIds, allele_synonyms, crossReferences)
                alleles = []
                allele_secondaryIds = []
                allele_synonyms = []
                crossReferences = []
                counter = 0

        if counter > 0:
            yield (alleles, allele_secondaryIds, allele_synonyms, crossReferences)
