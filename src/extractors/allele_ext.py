import uuid
from services import UrlService
from services import CreateCrossReference
from .resource_descriptor_ext import ResourceDescriptor


class AlleleExt(object):
    def get_alleles(self, allele_data, batch_size, testObject, graph):

        xrefUrlMap = ResourceDescriptor().get_data()

        list_to_yield = []
        dateProduced = allele_data['metaData']['dateProduced']
        dataProvider = allele_data['metaData']['dataProvider']
        release = ""

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
                                    CreateCrossReference.get_xref(local_crossref_id, prefix, page, page, crossRefId, modGlobalCrossRefId, crossRefId))
            allele_dataset = {
                "symbol": alleleRecord.get('symbol'),
                "geneId": alleleRecord.get('gene'),
                "primaryId": alleleRecord.get('primaryId'),
                "globalId": globalId,
                "localId": localId,
                "taxonId": alleleRecord.get('taxonId'),
                "synonyms": alleleRecord.get('synonyms'),
                "secondaryIds": alleleRecord.get('secondaryIds'),
                "dataProvider": dataProvider,
                "dateProduced": dateProduced,
                "loadKey": dataProvider + "_" + dateProduced + "_allele",
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
