import uuid
from services import UrlService
from services import SpeciesService
from .resource_descriptor_ext import ResourceDescriptor

class BGIExt(object):

    def get_page_complete_url(self, local_id, xrefUrlMap, prefix, page):

        individual_stanza_map = xrefUrlMap[prefix+page]

        page_url_prefix = individual_stanza_map["page_url_prefix"]
        page_url_suffix = individual_stanza_map["page_url_suffix"]

        complete_url = page_url_prefix + local_id + page_url_suffix

        return complete_url

    def get_no_page_complete_url(self, local_id, xrefUrlMap, prefix, page):
        individual_stanza_map = xrefUrlMap[prefix + page]

        default_url_prefix = individual_stanza_map["default_url_prefix"]
        default_url_suffix = individual_stanza_map["default_url_suffix"]

        complete_url = default_url_prefix + local_id + default_url_suffix

        return complete_url

    def get_data(self, gene_data, batch_size, testObject, graph):

        xrefUrlMap = ResourceDescriptor.get_data()
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

                                # special case yaml mismatch gene/interactions vs. gene/interaction from SGD TODO: fix this as SGD fixes
                                if page == 'gene/interaction':
                                    page = 'gene/interactions'

                                if page == 'gene/spell':
                                    page = 'gene/other_expression'

                                crossRefCompleteUrl = self.get_page_complete_url(local_crossref_id, xrefUrlMap, prefix, page)

                                if page == 'gene':
                                    modCrossReferenceCompleteUrl = UrlService.get_page_complete_url(local_crossref_id, crossRefId, primary_id, prefix + page, graph)
                                    geneticEntityExternalUrl = UrlService.get_page_complete_url(local_crossref_id, crossRefId, primary_id, prefix + page, graph)

                                if page == 'gene/references':
                                    geneLiteratureUrl = UrlService.get_page_complete_url(local_crossref_id, crossRefId, primary_id, prefix + page, graph)

                                # some MODs were a bit confused about whether or not to use "generic_cross_reference" or not.
                                # so we have to special case these for now.  TODO: fix generic_cross_reference in SGD, RGD

                                if page == 'generic_cross_reference':
                                    crossRefCompleteUrl = self.get_page_complete_url(local_crossref_id, xrefUrlMap, prefix, page)
                                crossReferences.append({
                                        "id": crossRef.get('id'),
                                        "globalCrossRefId": crossRef.get('id'),
                                        "localId": local_crossref_id,
                                        "crossRefCompleteUrl": crossRefCompleteUrl,
                                        "prefix": prefix,
                                        "crossRefType": page,
                                        "primaryKey": global_xref_id + page,
                                        "uuid": str(uuid.uuid4())
                                    })
                        else:
                            if prefix == 'PANTHER': # TODO Special Panther case needs to be handled in the resourceDescriptor.yaml
                                #TODO: add bucket for panther
                                crossRefPrimaryId = crossRef.get('id') + '_' + primary_id
                                crossReferences.append({
                                    "id": crossRefPrimaryId,
                                    "globalCrossRefId": crossRef.get('id'),
                                    "localId": local_crossref_id,
                                    "crossRefCompleteUrl": self.get_page_complete_url(local_crossref_id, xrefUrlMap, prefix, "default"),
                                    "prefix": prefix,
                                    "crossRefType": "gene/panther",
                                    "primaryKey": crossRefPrimaryId + "gene/panther",
                                    "uuid": str(uuid.uuid4())
                                })

                            else:
                                crossRefPrimaryId = crossRef.get('id')

                                crossReferences.append({
                                    "id": crossRefPrimaryId,
                                    "globalCrossRefId": crossRef.get('id'),
                                    "localId": local_crossref_id,
                                    "crossRefCompleteUrl": self.get_page_complete_url(local_crossref_id, xrefUrlMap, prefix, "default"),
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

