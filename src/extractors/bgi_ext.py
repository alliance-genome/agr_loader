import uuid
from services import SpeciesService
from services import UrlService
from services import CreateCrossReference
from services import DataProvider
from .resource_descriptor_ext import ResourceDescriptor
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BGIExt(object):

    def get_data(self, gene_data, batch_size, testObject, species):
        xrefUrlMap = ResourceDescriptor().get_data()
        list_to_yield = []

        dateProduced = gene_data['metaData']['dateProduced']
        dataProviders = []
        synonyms = []
        secondaryIds = []
        crossReferences = []
        genomicLocations = []
        gene_dataset = []
        release = None
        counter = 0

        dataProviderObject = gene_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        loadKey = dateProduced + dataProvider + "_BGI"

        # TODO: get SGD to fix their files.

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

        if 'release' in gene_data['metaData']:
            release = gene_data['metaData']['release']

        for geneRecord in gene_data['data']:
            counter = counter + 1


            primary_id = geneRecord['primaryId']
            global_id = geneRecord['primaryId']

            local_id = global_id.split(":")[1]
            geneLiteratureUrl = ""
            geneticEntityExternalUrl = ""
            modCrossReferenceCompleteUrl = ""
            taxonId = geneRecord.get("taxonId")

            if geneRecord['taxonId'] == "NCBITaxon:9606" or geneRecord['taxonId'] == "NCBITaxon:10090":
                local_id = geneRecord['primaryId']

            if testObject.using_test_data() is True:
                is_it_test_entry = testObject.check_for_test_id_entry(primary_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            #TODO: can we split this off into another class?

            if 'crossReferences' in geneRecord:
                for crossRef in geneRecord['crossReferences']:
                    if ':' in crossRef.get('id'):
                        crossRefId = crossRef.get('id')
                        localCrossRefId = crossRefId.split(":")[1]
                        prefix = crossRef.get('id').split(":")[0]
                        pages = crossRef.get('pages')
                        globalXrefId = crossRef.get('id')
                        displayName = globalXrefId

                        # some pages collection have 0 elements
                        if pages is not None and len(pages) > 0:
                            for page in pages:
                                modCrossReferenceCompleteUrl = ""
                                geneLiteratureUrl = ""

                                # TODO: fix this as SGD fixes
                                if page == 'gene/interaction':
                                    page = 'gene/interactions'

                                crossRefCompleteUrl = UrlService.get_page_complete_url(localCrossRefId,
                                                                                       xrefUrlMap, prefix, page)

                                if page == 'gene':
                                    modCrossReferenceCompleteUrl = UrlService.get_page_complete_url(localCrossRefId,
                                                                                            xrefUrlMap, prefix,
                                                                                            prefix + page)

                                geneticEntityExternalUrl = UrlService.get_page_complete_url(localCrossRefId, xrefUrlMap,
                                                                                      prefix, prefix + page)

                                if page == 'gene/references':
                                    geneLiteratureUrl = UrlService.get_page_complete_url(localCrossRefId, xrefUrlMap,
                                                                                   prefix, prefix + page)

                                if page == 'gene/spell':

                                    displayName='Serial Patterns of Expression Levels Locator (SPELL)'

                                # TODO: fix generic_cross_reference in SGD, RGD

                                if page == 'generic_cross_reference':
                                    crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId,
                                                                                              xrefUrlMap,
                                                                                              prefix, primary_id)

                                xrefMap = CreateCrossReference.get_xref(localCrossRefId, prefix, page,
                                                                  page, displayName, crossRefCompleteUrl,
                                                                      globalXrefId+page)
                                xrefMap['dataId'] = primary_id
                                crossReferences.append(xrefMap)

                        else:
                            if prefix == 'PANTHER':  # TODO handle in the resourceDescriptor.yaml
                                crossRefPrimaryId = crossRef.get('id') + '_' + primary_id
                                crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId, xrefUrlMap,
                                                                                          prefix, primary_id)

                                xrefMap = CreateCrossReference.get_xref(localCrossRefId, prefix, "gene/panther",
                                                                  "gene/panther", displayName, crossRefCompleteUrl,
                                                                  crossRefPrimaryId + "gene/panther")
                                xrefMap['dataId'] = primary_id
                                crossReferences.append(xrefMap)

                            else:
                                crossRefPrimaryId = crossRef.get('id')
                                crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId, xrefUrlMap,
                                                                                          prefix, primary_id)

                                xrefMap = CreateCrossReference.get_xref(localCrossRefId, prefix,
                                                                        "generic_cross_reference",
                                                                        "generic_cross_reference", displayName,
                                                                        crossRefCompleteUrl,
                                                                        crossRefPrimaryId + "generic_cross_reference")
                                xrefMap['dataId'] = primary_id
                                crossReferences.append(xrefMap)

            gene = {
                "symbol": geneRecord.get('symbol'),
                "name": geneRecord.get('name'),
                "geneticEntityExternalUrl": geneticEntityExternalUrl,
                "description": geneRecord.get('description'),
                "soTermId": geneRecord['soTermId'],
                "geneSynopsis": geneRecord.get('geneSynopsis'),
                "geneSynopsisUrl": geneRecord.get('geneSynopsisUrl'),
                "taxonId": geneRecord['taxonId'],
                "species": SpeciesService.get_species(taxonId),
                "geneLiteratureUrl": geneLiteratureUrl,
                "name_key": geneRecord.get('symbol'),
                "primaryId": primary_id,
                "category": "gene",
                "dateProduced": dateProduced,
                "dataProviders": dataProviders,
                "dataProvider": dataProviderSingle,
                "release": release,
                "href": None,
                "uuid": str(uuid.uuid4()),
                "modCrossRefCompleteUrl": modCrossReferenceCompleteUrl,
                "localId": local_id,
                "modGlobalCrossRefId": global_id,
                "modGlobalId": global_id,
                "loadKey": loadKey
            }
            gene_dataset.append(gene)

            if 'genomeLocations' in geneRecord:
                for genomeLocation in geneRecord['genomeLocations']:
                    chromosome = genomeLocation['chromosome']
                    assembly = genomeLocation['assembly']
                    if 'startPosition' in genomeLocation:
                        start = genomeLocation['startPosition']
                    else:
                        start = None
                    if 'endPosition' in genomeLocation:
                        end = genomeLocation['endPosition']
                    else:
                        end = None
                    if 'strand' in geneRecord['genomeLocations']:
                        strand = genomeLocation['strand']
                    else:
                        strand = None
                    #logger.info("gene id for locations")
                    #logger.info(primary_id)
                    genomicLocations.append(
                        {"primaryId": primary_id, "chromosome": chromosome, "start":
                            start, "end": end, "strand": strand, "assembly": assembly})
                    #logger.info(genomicLocations)

            if geneRecord.get('synonyms') is not None:
                for synonym in geneRecord.get('synonyms'):
                    geneSynonym = {
                        "primary_id": primary_id,
                        "synonym": synonym
                    }
                    synonyms.append(geneSynonym)

            if geneRecord.get('secondaryIds') is not None:
                for secondaryId in geneRecord.get('secondaryIds'):
                    geneSecondaryId = {
                        "primary_id": primary_id,
                        "secondary_id": secondaryId
                    }
                    secondaryIds.append(geneSecondaryId)
            
            # Establishes the number of genes to yield (return) at a time.
            list_to_yield.append(gene_dataset)
            if counter == batch_size:
                logger.info("size of genomicLocations")
                logger.info(len(genomicLocations))
                yield (gene_dataset, synonyms, secondaryIds, genomicLocations, crossReferences)
                list_to_yield[:] = []  # Empty the list.
                gene_dataset = []
                synonyms = []
                secondaryIds = []
                genomicLocations = []
                crossReferences = []

        if counter > 0:
            logger.info("size of genomicLocations")
            logger.info(len(genomicLocations))
            yield (gene_dataset, synonyms, secondaryIds, genomicLocations, crossReferences)

