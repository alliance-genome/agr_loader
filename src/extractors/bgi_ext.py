import uuid
from services import UrlService
from services import SpeciesService

class BGIExt(object):


    def get_data(self, gene_data, batch_size, testObject, graph):

        gene_dataset = {}
        list_to_yield = []

        dateProduced = gene_data['metaData']['dateProduced']
        dataProvider = gene_data['metaData']['dataProvider']
        release = None

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
                        local_crossref_id = crossRefId.split(":")[1]
                        prefix = crossRef.get('id').split(":")[0]
                        pages = crossRef.get('pages')
                        global_xref_id = crossRef.get('id')

                        # some pages collection have 0 elements
                        if pages is not None and len(pages) > 0:
                            for page in pages:
                                modCrossReferenceCompleteUrl = ""
                                geneticEntityExternalUrl = ""
                                geneLiteratureUrl = ""

                                crossReferences.append({
                                    "id": crossRef.get('id'),
                                    "globalCrossRefId": crossRef.get('id'),
                                    "localId": local_crossref_id,
                                    "crossRefCompleteUrl": UrlService.get_page_complete_url(local_crossref_id, crossRefId, primary_id, prefix + page, graph),
                                    "prefix": prefix,
                                    "crossRefType": page,
                                    "primaryKey": global_xref_id + page,
                                    "uuid": str(uuid.uuid4())
                                })
                                if page == 'gene':
                                    modCrossReferenceCompleteUrl = UrlService.get_page_complete_url(local_crossref_id, crossRefId, primary_id, prefix + page, graph)
                                    geneticEntityExternalUrl = UrlService.get_page_complete_url(local_crossref_id, crossRefId, primary_id, prefix + page, graph)

                                if page == 'gene/references':
                                    geneLiteratureUrl = UrlService.get_page_complete_url(local_crossref_id, crossRefId, primary_id, prefix + page, graph)
                        else:
                            if prefix == 'PANTHER': # TODO Special Panther case to be addressed post 1.0
                                #TODO: add bucket for panther
                                crossRefPrimaryId = crossRef.get('id') + '_' + primary_id
                            else:
                                crossRefPrimaryId = crossRef.get('id')

                            crossReferences.append({
                                "id": crossRefPrimaryId,
                                "globalCrossRefId": crossRef.get('id'),
                                "localId": local_crossref_id,
                                "crossRefCompleteUrl": UrlService.get_no_page_complete_url(local_crossref_id, crossRefId, primary_id, prefix+"default", graph),
                                "prefix": prefix,
                                "crossRefType": "generic_cross_reference",
                                "primaryKey": crossRefPrimaryId + "generic_cross_reference",
                                "uuid": str(uuid.uuid4())
                                })

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
                "dataProvider": dataProvider,
                "release": release,
                "href": None,
                "uuid": str(uuid.uuid4()),
                "modCrossRefCompleteUrl": modCrossReferenceCompleteUrl,
                "localId": local_id,
                "modGlobalCrossRefId": global_id,
                "modGlobalId": global_id,
                "loadKey": dataProvider+"_"+dateProduced+"_BGI"
            }
            
            # Establishes the number of genes to yield (return) at a time.
            list_to_yield.append(gene_dataset)
            if len(list_to_yield) == batch_size:
                yield list_to_yield
                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield

