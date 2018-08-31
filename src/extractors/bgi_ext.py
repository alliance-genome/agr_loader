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
        release = None

        for dataProviderObject in gene_data['metaData']['dataProvider']:

            dataProviderCrossRef = dataProviderObject.get('crossReference')
            dataProvider = dataProviderCrossRef.get('id')
            dataProviderPages = dataProviderCrossRef.get('pages')
            dataProviderCrossRefSet = []

            loadKey = dateProduced + dataProvider + "_BGI"

            #TODO: get SGD to fix their files.
            if dataProviderPages != None:
                for dataProviderPage in dataProviderPages:
                    crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
                                                                       dataProviderPage)
                    dataProviderCrossRefSet.append(
                        CreateCrossReference.get_xref(dataProvider, dataProvider, dataProviderPage,
                                                  dataProviderPage, dataProvider, crossRefCompleteUrl,
                                                  dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.info ("data provider: " + dataProvider)

        dataProviderSingle = DataProvider().get_data_provider(species)

        if 'release' in gene_data['metaData']:
            release = gene_data['metaData']['release']

        for geneRecord in gene_data['data']:

            crossReferences = []
            genomic_locations = []

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

                                crossRefCompleteUrl = UrlService.get_page_complete_url(localCrossRefId, xrefUrlMap, prefix, page)

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
                                    crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId, xrefUrlMap, prefix, primary_id)

                                crossReferences.append(
                                        CreateCrossReference.get_xref(localCrossRefId, prefix, page,
                                                                  page, displayName, crossRefCompleteUrl, globalXrefId+page))

                        else:
                            if prefix == 'PANTHER': # TODO Special Panther case needs to be handled in the resourceDescriptor.yaml
                                #TODO: add bucket for panther
                                crossRefPrimaryId = crossRef.get('id') + '_' + primary_id
                                crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId, xrefUrlMap,
                                                                                          prefix, primary_id)

                                crossReferences.append(CreateCrossReference.get_xref(localCrossRefId, prefix, "gene/panther","gene/panther", displayName, crossRefCompleteUrl, crossRefPrimaryId+"gene/panther"))


                            else:
                                crossRefPrimaryId = crossRef.get('id')
                                crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId, xrefUrlMap,
                                                                                          prefix, primary_id)
                                crossReferences.append(
                                    CreateCrossReference.get_xref(localCrossRefId, prefix, "generic_cross_reference",
                                                                  "generic_cross_reference", displayName, crossRefCompleteUrl, crossRefPrimaryId+"generic_cross_reference"))

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
                    genomic_locations.append(
                        {"geneLocPrimaryId": primary_id, "chromosome": chromosome, "start": start, "end": end, "strand": strand, "assembly": assembly})

            gene_dataset = {
                "symbol": geneRecord['symbol'],
                "name": geneRecord.get('name'),
                "geneticEntityExternalUrl": geneticEntityExternalUrl,
                "description": geneRecord.get('description'),
                "synonyms": geneRecord.get('synonyms'),
                "soTermId": geneRecord['soTermId'],
                "soTermName": None,
                "diseases": [],
                "secondaryIds": geneRecord.get('secondaryIds'),
                "geneSynopsis": geneRecord.get('geneSynopsis'),
                "geneSynopsisUrl": geneRecord.get('geneSynopsisUrl'),
                "taxonId": geneRecord['taxonId'],
                "species": SpeciesService.get_species(taxonId),
                "genomeLocations": genomic_locations,
                "geneLiteratureUrl": geneLiteratureUrl,
                "name_key": geneRecord['symbol'],
                "primaryId": primary_id,
                "crossReferences": crossReferences,
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
            
            # Establishes the number of genes to yield (return) at a time.
            list_to_yield.append(gene_dataset)
            if len(list_to_yield) == batch_size:
                yield list_to_yield
                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield

