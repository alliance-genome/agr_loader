'''BGI ETL'''

import logging
import sys

import uuid
import multiprocessing
from etl import ETL
from etl.helpers import ETLHelper
from transactors import CSVTransactor, Neo4jTransactor
from files import JSONFile

class BGIETL(ETL):
    '''BGI ETL'''

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    so_terms_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:Gene {primaryKey:row.primaryKey})
            MATCH (s:SOTerm:Ontology {primaryKey:row.soTermId})
            MERGE (o)-[:ANNOTATED_TO]->(s)"""

    chromosomes_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MERGE (chrm:Chromosome {primaryKey: row.primaryKey}) """

    genomic_locations_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Gene {primaryKey:row.primaryId})
            MATCH (chrm:Chromosome {primaryKey:row.chromosome})

            MERGE (o)-[ochrm:LOCATED_ON]->(chrm)            
            MERGE (a:Assembly {primaryKey:row.assembly})
            
            MERGE (gchrm:GenomicLocation {primaryKey:row.uuid})
            ON CREATE SET gchrm.start = apoc.number.parseInt(row.start),
                gchrm.end = apoc.number.parseInt(row.end),
                gchrm.assembly = row.assembly,
                gchrm.strand = row.strand,
                gchrm.chromosome = row.chromosome,
                gchrm.assembly = row.assembly
                
            MERGE (o)-[of:ASSOCIATION]-(gchrm)
            MERGE (gchrm)-[ofc:ASSOCIATION]-(chrm)
            
        """

    genomic_locations_bins_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Gene {primaryKey:row.genePrimaryId})
            MATCH (chrm:Chromosome {primaryKey:row.chromosome})

            MERGE (bin:GenomicLocationBin {primaryKey:row.binPrimaryKey})
            ON CREATE SET bin.number = toInt(row.number),
               bin.assembly = row.assembly

            MERGE (o)-[:LOCATED_IN]->(bin)
            MERGE (bin)-[:LOCATED_ON]->(chrm) """

    gene_secondary_ids_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey:row.primary_id})
            
            MERGE (second:SecondaryId:Identifier {primaryKey:row.secondary_id})
                ON CREATE SET second.name = row.secondary_id
            MERGE (g)-[aka1:ALSO_KNOWN_AS]->(second) """

    gene_synonyms_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey:row.primary_id})
                
            MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                    SET syn.name = row.synonym
            MERGE (g)-[aka2:ALSO_KNOWN_AS]->(syn) """

    gene_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (l:Load {primaryKey:row.loadKey})
            
            //Create the Gene node and set properties. primaryKey is required.
            MERGE (o:Gene {primaryKey:row.primaryId})
                ON CREATE SET o.symbol = row.symbol,
                              o.taxonId = row.taxonId,
                              o.name = row.name,
                              o.description = row.description,
                              o.geneSynopsisUrl = row.geneSynopsisUrl,
                              o.geneSynopsis = row.geneSynopsis,
                              o.geneLiteratureUrl = row.geneLiteratureUrl,
                              o.geneticEntityExternalUrl = row.geneticEntityExternalUrl,
                              o.dateProduced = row.dateProduced,
                              o.modGlobalCrossRefId = row.modGlobalCrossRefId,
                              o.modCrossRefCompleteUrl = row.modCrossRefCompleteUrl,
                              o.modLocalId = row.localId,
                              o.modGlobalId = row.modGlobalId,
                              o.uuid = row.uuid,
                              o.dataProvider = row.dataProvider,
                              o.symbolWithSpecies = row.symbolWithSpecies
                              
            MERGE (spec:Species {primaryKey: row.taxonId})
              ON CREATE SET spec.species = row.species, 
                            spec.name = row.species,
                            spec.phylogeneticOrder = apoc.number.parseInt(row.speciesPhylogeneticOrder)
    """

    basic_gene_load_relations_query_template = """
    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
        MATCH (l:Load {primaryKey:row.loadKey})
        MATCH (g:Gene {primaryKey:row.primaryId})
        MERGE (g)-[:LOADED_FROM]->(l) 
    
    """

    basic_gene_species_relations_query_template = """
    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
        MATCH (spec:Species {primaryKey: row.taxonId})
        MATCH (g:Gene {primaryKey: row.primaryId})

        MERGE (g)-[:FROM_SPECIES]->(spec)

    """

    xrefs_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Gene {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_xref_tuned_text()

    xrefs_relationships_query_template = """
    
        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Gene {primaryKey:row.dataId})
            MATCH (c:CrossReference {primaryKey:row.primaryKey})
            
            MERGE (o)-[oc:CROSS_REFERENCE]-(c)
            
    """ + ETLHelper.merge_crossref_relationships()


    gene_metadata_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        //Create the load node(s)
        CREATE (l:Load:Entity {primaryKey:row.loadKey})
            SET l.dateProduced = row.dateProduced,
                l.loadName = "BGI",
                l.release = row.release,
                l.dataProviders = row.dataProviders,
                l.dataProvider = row.dataProvider
        """

    def __init__(self, config):
        self.metadata_is_loaded = {} # Dictionary for optimizing metadata loading.
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        thread_pool = []

        query_tracking_list = multiprocessing.Manager().list()
        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type,
                                              args=(sub_type, query_tracking_list))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

        queries = []
        for item in query_tracking_list:
            queries.append(item)

        Neo4jTransactor.execute_query_batch(queries)

    def _process_sub_type(self, sub_type, query_tracking_list):

        self.logger.info("Loading BGI Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        if filepath is None:
            self.logger.error("Can't find input file for %s", sub_type)
            sys.exit()

        data = JSONFile().get_data(filepath)

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # gene_metadata, gene_dataset, secondary_ids, genomic_locations, cross_references, synonyms
        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.gene_metadata_query_template, commit_size,
             "gene_metadata_" + sub_type.get_data_provider() + ".csv"],
            [self.gene_query_template, commit_size,
             "gene_data_" + sub_type.get_data_provider() + ".csv"],
            [self.basic_gene_load_relations_query_template, commit_size,
             "gene_data_load_" + sub_type.get_data_provider() + ".csv"],
            [self.basic_gene_species_relations_query_template, commit_size,
             "gene_data_species_" + sub_type.get_data_provider() + ".csv"],
            [self.so_terms_query_template, commit_size,
             "gene_so_terms_" + sub_type.get_data_provider() + ".csv"],
            [self.chromosomes_query_template, commit_size,
             "gene_chromosomes_" + sub_type.get_data_provider() + ".csv"],
            [self.gene_secondary_ids_query_template, commit_size,
             "gene_secondary_ids_" + sub_type.get_data_provider() + ".csv"],
            [self.genomic_locations_query_template, commit_size,
             "gene_genomic_locations_" + sub_type.get_data_provider() + ".csv"],
            [self.xrefs_query_template, commit_size,
             "gene_cross_references_" + sub_type.get_data_provider() + ".csv"],
            [self.xrefs_relationships_query_template, commit_size,
             "gene_cross_references_relationships_" + sub_type.get_data_provider() + ".csv"],
            [self.gene_synonyms_query_template, 600000,
             "gene_synonyms_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)

        for item in query_and_file_list:
            query_tracking_list.append(item)

        self.logger.info("Finished Loading BGI Data: %s",
                         sub_type.get_data_provider())

    def get_generators(self, gene_data, data_provider, batch_size):
        '''Create Generators'''

        date_produced = gene_data['metaData']['dateProduced']
        synonyms = []
        secondary_ids = []
        cross_references = []
        #xref_relations = []
        genomic_locations = []
        #genomic_locationBins = []
        gene_dataset = []
        gene_metadata = []
        gene_to_so_terms = []
        chromosomes = {}
        release = None
        counter = 0

        data_provider_object = gene_data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')
        # dataProviderPages = dataProviderCrossRef.get('pages')
        # data_provider_cross_ref_set = []

        load_key = date_produced + data_provider + "_BGI"

        # If we're not tracking the metadata, create the entry in our tracker.
        if not load_key in self.metadata_is_loaded:
            self.metadata_is_loaded[load_key] = False

        # if dataProviderPages is not None:
        #     for dataProviderPage in dataProviderPages:
        #         cross_ref_complete_url = UrlService.get_page_complete_url(data_provider,
        #                                                                   ETL.xref_url_map,
        #                                                                   data_provider,
        #                                                                   data_provider_page)
        #         data_provider_cross_ref_set.append(ETLHelper.get_xref_dict(\
        #              data_provider,
        #              data_provider,
        #              data_provider_page,
        #              data_provider_page,
        #              data_provider,
        #              cross_ref_complete_url,
        #              data_provider + data_provider_page))
        #         data_providers.append(data_provider)
        #         self.logger.info("BGI using data provider: " + data_provider)

        if 'release' in gene_data['metaData']:
            release = gene_data['metaData']['release']

        if self.metadata_is_loaded[load_key] is False:
            gene_metadata = []
            metadata_dict = {
                'loadKey' : load_key,
                'loadName' : 'BGI',
                'release' : release,
                'dataProviders' : None,
                'dataProvider' : data_provider
            }
            gene_metadata.append(metadata_dict)

        for gene_record in gene_data['data']:
            counter = counter + 1
            basic_genetic_entity = gene_record['basicGeneticEntity']
            primary_id = basic_genetic_entity.get('primaryId')
            global_id = basic_genetic_entity.get('primaryId')
            local_id = global_id.split(":")[1]
            gene_literature_url = ""
            genetic_entity_external_url = ""
            mod_cross_reference_complete_url = ""
            taxon_id = basic_genetic_entity.get("taxonId")
            short_species_abbreviation = ETLHelper.get_short_species_abbreviation(taxon_id)


            if basic_genetic_entity.get('taxonId') in ["NCBITaxon:9606", "NCBITaxon:10090"]:
                local_id = basic_genetic_entity.get('primaryId')

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(primary_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            if 'crossReferences' in basic_genetic_entity:
                for cross_ref in basic_genetic_entity.get('crossReferences'):
                    if ':' in cross_ref.get('id'):
                        cross_ref_id = cross_ref.get('id')
                        local_cross_ref_id = cross_ref_id.split(":")[1]
                        prefix = cross_ref.get('id').split(":")[0]
                        pages = cross_ref.get('pages')
                        global_xref_id = cross_ref.get('id')
                        display_name = global_xref_id

                        # some pages collection have 0 elements
                        if pages is not None and len(pages) > 0:
                            for page in pages:
                                mod_cross_reference_complete_url = ""
                                gene_literature_url = ""
                                display_name = ""

                                cross_ref_complete_url = ETLHelper.get_page_complete_url(\
                                        local_cross_ref_id,
                                        ETL.xref_url_map,
                                        prefix,
                                        page)

                                if page == 'gene/expression_images':
                                    cross_ref_complete_url = ETLHelper.get_expression_images_url(\
                                            local_cross_ref_id,
                                            cross_ref_id)
                                elif page == 'gene':
                                    mod_cross_reference_complete_url \
                                        = ETLHelper.get_page_complete_url(local_cross_ref_id,
                                                                          ETL.xref_url_map,
                                                                          prefix,
                                                                          prefix + page)

                                genetic_entity_external_url = ETLHelper.get_page_complete_url(\
                                        local_cross_ref_id,
                                        ETL.xref_url_map,
                                        prefix,
                                        prefix + page)

                                if page == 'gene/references':
                                    gene_literature_url = ETLHelper.get_page_complete_url(\
                                            local_cross_ref_id,
                                            ETL.xref_url_map,
                                            prefix,
                                            prefix + page)

                                if page == 'gene/spell':
                                    display_name = \
                                            'Serial Patterns of Expression Levels Locator (SPELL)'

                                # TODO: fix generic_cross_reference in SGD, RGD

                                if page == 'generic_cross_reference':
                                    cross_ref_complete_url = ETLHelper.get_no_page_complete_url(\
                                            local_cross_ref_id,
                                            ETL.xref_url_map,
                                            prefix,
                                            primary_id)

                                # TODO: fix gene/disease xrefs for SGD once
                                # resourceDescriptor change in develop
                                # makes its way to the release branch.

                                if page == 'gene/disease' and taxon_id == 'NCBITaxon:559292':
                                    cross_ref_complete_url = '/'.join(
                                        ["https://www.yeastgenome.org",
                                         "locus",
                                         local_id,
                                         "disease"])

                                xref_map = ETLHelper.get_xref_dict(local_cross_ref_id,
                                                                   prefix,
                                                                   page,
                                                                   page,
                                                                   display_name,
                                                                   cross_ref_complete_url,
                                                                   global_xref_id + page)
                                xref_map['dataId'] = primary_id
                                cross_references.append(xref_map)

                        else:
                            # TODO handle in the resourceDescriptor.yaml
                            if prefix == 'PANTHER':                     
                                cross_ref_primary_id = cross_ref.get('id') + '_' + primary_id
                                cross_ref_complete_url = ETLHelper.get_no_page_complete_url(\
                                        local_cross_ref_id,
                                        ETL.xref_url_map,
                                        prefix,
                                        primary_id)
                                xref_map = ETLHelper.get_xref_dict(\
                                        local_cross_ref_id,
                                        prefix,
                                        "gene/panther",
                                        "gene/panther",
                                        display_name,
                                        cross_ref_complete_url,
                                        cross_ref_primary_id + "gene/panther")
                                xref_map['dataId'] = primary_id
                                cross_references.append(xref_map)
                            # TODO handle human generic cross reference to RGD in resourceDescr.
                            elif prefix == 'RGD':
                                cross_ref_primary_id = cross_ref.get('id')
                                cross_ref_complete_url = "https://rgd.mcw.edu" \
                                                         + "/rgdweb/elasticResults.html?term=" \
                                                         + local_cross_ref_id

                                xref_map = ETLHelper.get_xref_dict(\
                                        local_cross_ref_id,
                                        prefix,
                                        "generic_cross_reference",
                                        "generic_cross_reference",
                                        display_name,
                                        cross_ref_complete_url,
                                        cross_ref_primary_id + "generic_cross_reference")
                                xref_map['dataId'] = primary_id
                                cross_references.append(xref_map)

                            else:
                                cross_ref_primary_id = cross_ref.get('id')
                                cross_ref_complete_url = ETLHelper.get_no_page_complete_url(\
                                        local_cross_ref_id,
                                        ETL.xref_url_map,
                                        prefix,
                                        primary_id)
                                xref_map = ETLHelper.get_xref_dict(\
                                        local_cross_ref_id,
                                        prefix,
                                        "generic_cross_reference",
                                        "generic_cross_reference",
                                        display_name,
                                        cross_ref_complete_url,
                                        cross_ref_primary_id + "generic_cross_reference")
                                xref_map['dataId'] = primary_id
                                cross_references.append(xref_map)

            # TODO Metadata can be safely removed from this dictionary. Needs to be tested.

            gene_to_so_terms.append({
                "primaryKey": primary_id,
                "soTermId": gene_record['soTermId']})

            gene = {
                "symbol": gene_record.get('symbol'),
                # globallyUniqueSymbolWithSpecies requested by search group
                "symbolWithSpecies": gene_record.get('symbol')\
                                     + " (" + short_species_abbreviation + ")",
                "name": gene_record.get('name'),
                "geneticEntityExternalUrl": genetic_entity_external_url,
                "description": gene_record.get('description'),
                "geneSynopsis": gene_record.get('geneSynopsis'),
                "geneSynopsisUrl": gene_record.get('geneSynopsisUrl'),
                "taxonId": basic_genetic_entity.get('taxonId'),
                "species": ETLHelper.species_lookup_by_taxonid(taxon_id),
                "speciesPhylogeneticOrder": ETLHelper.get_species_order(taxon_id),
                "geneLiteratureUrl": gene_literature_url,
                "name_key": gene_record.get('symbol'),
                "primaryId": primary_id,
                "category": "gene",
                "href": None,
                "uuid": str(uuid.uuid4()),
                "modCrossRefCompleteUrl": mod_cross_reference_complete_url,
                "localId": local_id,
                "modGlobalCrossRefId": global_id,
                "modGlobalId": global_id,
                "loadKey" : load_key,
                "dataProvider" : data_provider,
                "dateProduced": date_produced}

            gene_dataset.append(gene)

            if 'genomeLocations' in basic_genetic_entity:
                for genome_location in basic_genetic_entity.get('genomeLocations'):
                    chromosome = genome_location.get('chromosome')
                    if chromosome is not None:
                        if chromosome.startswith("chr"):
                            chromosome = chromosome[3:]

                        if chromosome not in chromosomes:
                            chromosomes[chromosome] = {"primaryKey": chromosome}

                        if 'startPosition' in genome_location:
                            start = genome_location['startPosition']
                        else:
                            start = None

                        if 'endPosition' in genome_location:
                            end = genome_location['endPosition']
                        else:
                            end = None

                        if 'strand' in basic_genetic_entity['genomeLocations']:
                            strand = genome_location['strand']
                        else:
                            strand = None

                    assembly = genome_location.get('assembly')

                    if 'strand' in genome_location:
                        strand = genome_location['strand']
                    else:
                        strand = None

#                    if primary_id and start and end and assembly and chromosome:
#                        bin_size = 2000
#                        start_int = int(start)
#                        end_int = int(end)
#                        if start_int < endInt:
#                            min_coordinate = start_int
#                            max_coordinate = end_int
#                        else:
#                            min_coordinate = end_int
#                            max_coordinate = start_int
#
#                        start_bin = math.floor(min_coordinate / bin_size)
#                        end_bin = math.ceil(max_coordinate / bin_size)
#
#                        for bin_number in list(range(start_bin, end_bin)):
#                            bin_primary_key = '\'.join([taxon_id,
#                                                        assembly,
#                                                        chromosome,
#                                                        str(bin_number)]))
#                            genomic_location_bins.append({"bin_primary_key": bin_primary_key,
#                                                          "gene_primary_id": primary_id,
#                                                          "chromosome": chromosome,
#                                                          "taxon_id": taxon_id,
#                                                          "assembly": assembly,
#                                                          "number": bin_number})

                    genomic_locations.append({"primaryId": primary_id,
                                              "chromosome": chromosome,
                                              "start": start,
                                              "end": end,
                                              "strand": strand,
                                              "assembly": assembly,
                                              "uuid": str(uuid.uuid4())})

            if basic_genetic_entity.get('synonyms') is not None:
                for synonym in basic_genetic_entity.get('synonyms'):
                    gene_synonym = {
                        "primary_id": primary_id,
                        "synonym": synonym.strip()}
                    synonyms.append(gene_synonym)

            if basic_genetic_entity.get('secondaryIds') is not None:
                for secondary_id in basic_genetic_entity.get('secondaryIds'):
                    gene_secondary_id = {
                        "primary_id": primary_id,
                        "secondary_id": secondary_id}
                    secondary_ids.append(gene_secondary_id)

            # We should have the metadata ready to go after the first loop of the generator.
            self.metadata_is_loaded[load_key] = True

            # Establishes the number of genes to yield (return) at a time.
            if counter == batch_size: # only sending unique chromosomes, hense empty list here.
                counter = 0
                yield [gene_metadata,
                       gene_dataset,
                       gene_dataset,
                       gene_dataset,
                       gene_to_so_terms,
                       [],
                       secondary_ids,
                       genomic_locations,
                       cross_references,
                       cross_references,
                       synonyms]
                gene_metadata = []
                gene_dataset = []
                synonyms = []
                secondary_ids = []
                genomic_locations = []
                cross_references = []
                gene_to_so_terms = []
                #xref_relations = []

        if counter > 0:
            yield [gene_metadata,
                   gene_dataset,
                   gene_dataset,
                   gene_dataset,
                   gene_to_so_terms,
                   chromosomes.values(),
                   secondary_ids,
                   genomic_locations,
                   cross_references,
                   cross_references,
                   synonyms]
