"""BGI ETL."""

import logging
import sys

import uuid
import multiprocessing
from etl import ETL
from etl.helpers import ETLHelper
from transactors import CSVTransactor, Neo4jTransactor
from files import JSONFile


class BGIETL(ETL):
    """BGI ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    so_terms_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:Gene {primaryKey:row.primaryKey})
            MATCH (s:SOTerm {primaryKey:row.soTermId})
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
              ON CREATE SET a.dataProvider = row.dataProvider

            MERGE (gchrm:GenomicLocation {primaryKey:row.uuid})
            ON CREATE SET gchrm.start = apoc.number.parseInt(row.start),
                gchrm.end = apoc.number.parseInt(row.end),
                gchrm.assembly = row.assembly,
                gchrm.strand = row.strand,
                gchrm.chromosome = row.chromosome,
                gchrm.assembly = row.assembly

            MERGE (o)-[of:ASSOCIATION]-(gchrm)
            MERGE (gchrm)-[ofc:ASSOCIATION]-(chrm)
            MERGE (gchrmn)-[ao:ASSOCIATION]->(a)

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
        """Initialise object."""
        self.metadata_is_loaded = {}  # Dictionary for optimizing metadata loading.
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
            [self.gene_metadata_query_template, commit_size, "gene_metadata_" + sub_type.get_data_provider() + ".csv"],
            [self.gene_query_template, commit_size, "gene_data_" + sub_type.get_data_provider() + ".csv"],
            [self.basic_gene_load_relations_query_template, commit_size, "gene_data_load_" + sub_type.get_data_provider() + ".csv"],
            [self.basic_gene_species_relations_query_template, commit_size, "gene_data_species_" + sub_type.get_data_provider() + ".csv"],
            [self.so_terms_query_template, commit_size, "gene_so_terms_" + sub_type.get_data_provider() + ".csv"],
            [self.chromosomes_query_template, commit_size, "gene_chromosomes_" + sub_type.get_data_provider() + ".csv"],
            [self.gene_secondary_ids_query_template, commit_size, "gene_secondary_ids_" + sub_type.get_data_provider() + ".csv"],
            [self.genomic_locations_query_template, commit_size, "gene_genomic_locations_" + sub_type.get_data_provider() + ".csv"],
            [self.xrefs_query_template, commit_size, "gene_cross_references_" + sub_type.get_data_provider() + ".csv"],
            [self.xrefs_relationships_query_template, commit_size, "gene_cross_references_relationships_" + sub_type.get_data_provider() + ".csv"],
            [self.gene_synonyms_query_template, commit_size, "gene_synonyms_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)

        for item in query_and_file_list:
            query_tracking_list.append(item)

        self.error_messages("BGI-{}: ".format(sub_type.get_data_provider()))
        self.logger.info("Finished Loading BGI Data: %s", sub_type.get_data_provider())

    def secondary_process(self, secondarys, data_record):
        """Get secondary ids.

        secondarys: list of dataset items.
        data_record: record to process.
        """
        if data_record.get('secondaryIds') is None:
            return
        for sid in data_record.get('secondaryIds'):
            secondary_id_dataset = {
                "primary_id": data_record.get('primaryId'),
                "secondary_id": sid
            }
            secondarys.append(secondary_id_dataset)

    def synonyms_process(self, synonyms, data_record):
        """Get synonyms."""
        if data_record.get('synonyms') is None:
            return
        for syn in data_record.get('synonyms'):
            syn_dataset = {
                "primary_id": data_record.get('primaryId'),
                "synonym": syn.strip()
            }
            synonyms.append(syn_dataset)

    def xref_process(self, basic_genetic_entity, cross_references, urls, data_provider):
        """Process xrefs."""
        primary_id = basic_genetic_entity.get('primaryId')
        global_id = basic_genetic_entity.get('primaryId')
        local_id = global_id.split(":")[1]
        taxon_id = basic_genetic_entity.get("taxonId")
        if 'crossReferences' not in basic_genetic_entity:
            return
        for cross_ref in basic_genetic_entity.get('crossReferences'):
            if ':' not in cross_ref.get('id'):
                continue
            cross_ref_id = cross_ref.get('id')
            local_cross_ref_id = cross_ref_id.split(":")[1]
            prefix = cross_ref.get('id').split(":")[0]
            pages = cross_ref.get('pages')
            global_xref_id = cross_ref.get('id')
            display_name = global_xref_id

            # some pages collection have 0 elements
            if pages is not None and len(pages) > 0:
                for page in pages:
                    display_name = ""

                    # TODO remove when MGI updates resourceDescriptor to remove plural.

                    if page == 'gene/phenotype':
                        page = 'gene/phenotypes'

                    if page == 'gene/phenotypes':
                        display_name = data_provider
                    cross_ref_complete_url = self.etlh.rdh2.return_url_from_key_value(
                        prefix, local_cross_ref_id, page)

                    if page == 'gene/expression_images':
                        cross_ref_complete_url = self.etlh.rdh2.return_url_from_key_value(
                            prefix, local_cross_ref_id, page)
                    elif page == 'gene':
                        urls['mod_cross_reference_complete_url'] = self.etlh.rdh2.return_url_from_key_value(
                            prefix, local_cross_ref_id, page)

                    urls['genetic_entity_external_url'] = self.etlh.rdh2.return_url_from_key_value(
                            prefix, local_cross_ref_id, page)

                    if page == 'gene/references':
                        urls['gene_literature_url'] = self.etlh.rdh2.return_url_from_key_value(
                            prefix, local_cross_ref_id, page)

                    if page == 'gene/spell':
                        display_name = 'Serial Patterns of Expression Levels Locator (SPELL)'

                    if page == 'gene/phenotypes_impc':
                        display_name = 'IMPC'

                    # TODO: fix generic_cross_reference in SGD, RGD

                    if page == 'generic_cross_reference':
                        cross_ref_complete_url = self.etlh.get_no_page_complete_url(
                            local_cross_ref_id, prefix, primary_id)

                    # TODO: fix gene/disease xrefs for SGD once
                    # resourceDescriptor change in develop
                    # makes its way to the release branch.

                    if page == 'gene/disease' and taxon_id == 'NCBITaxon:559292':
                        cross_ref_complete_url = self.etlh.rdh2.return_url_from_key_value('SGD', local_id, page)

                    xref_map = ETLHelper.get_xref_dict(
                        local_cross_ref_id,
                        prefix,
                        page,
                        page,
                        display_name,
                        cross_ref_complete_url,
                        global_xref_id + page)
                    xref_map['dataId'] = primary_id
                    cross_references.append(xref_map)
            else:
                if prefix == 'PANTHER':
                    cross_ref_primary_id = cross_ref.get('id') + '_' + primary_id
                    cross_ref_complete_url = self.etlh.get_no_page_complete_url(
                        local_cross_ref_id, prefix, primary_id)
                    page = "gene/panther"
                elif prefix == 'RGD':
                    cross_ref_primary_id = cross_ref.get('id')
                    cross_ref_complete_url = self.etlh.rdh2.return_url_from_key_value('RGD', local_cross_ref_id)
                    page = "generic_cross_reference"
                else:
                    cross_ref_primary_id = cross_ref.get('id')
                    cross_ref_complete_url = self.etlh.get_no_page_complete_url(
                        local_cross_ref_id, prefix, primary_id)
                    page = "generic_cross_reference"
                xref_map = ETLHelper.get_xref_dict(
                    local_cross_ref_id,
                    prefix,
                    page,
                    page,
                    display_name,
                    cross_ref_complete_url,
                    cross_ref_primary_id + page)
                xref_map['dataId'] = primary_id
                cross_references.append(xref_map)

    def locations_process(self, basic_genetic_entity, chromosomes, genomic_locations):
        """Get chromosome and genomic location info."""
        primary_id = basic_genetic_entity.get('primaryId')

        if 'genomeLocations' not in basic_genetic_entity:
            return
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

            genomic_locations.append({"primaryId": primary_id,
                                      "chromosome": chromosome,
                                      "start": start,
                                      "end": end,
                                      "strand": strand,
                                      "assembly": assembly,
                                      "uuid": str(uuid.uuid4()),
                                      "dataProvider": self.data_provider})

    def get_generators(self, gene_data, data_provider, batch_size):
        """Create Generators."""
        date_produced = gene_data['metaData']['dateProduced']
        synonyms = []
        secondary_ids = []
        cross_references = []
        genomic_locations = []
        gene_dataset = []
        gene_metadata = []
        gene_to_so_terms = []
        chromosomes = {}
        release = None
        counter = 0

        # small hack to fix gene descriptions while we discuss what to do here w/re to conversion from using
        # meta data in the input files to using subType config version.

        if data_provider == 'HUMAN':
            data_provider = 'RGD'

        self.data_providers_process(gene_data)
        load_key = date_produced + data_provider + "_BGI"

        # If we're not tracking the metadata, create the entry in our tracker.
        if load_key not in self.metadata_is_loaded:
            self.metadata_is_loaded[load_key] = False

        if 'release' in gene_data['metaData']:
            release = gene_data['metaData']['release']

        if self.metadata_is_loaded[load_key] is False:
            gene_metadata = []
            metadata_dict = {
                'loadKey': load_key,
                'loadName': 'BGI',
                'release': release,
                'dataProviders': None,
                'dataProvider': data_provider
            }
            gene_metadata.append(metadata_dict)

        for gene_record in gene_data['data']:
            counter = counter + 1
            urls = {'gene_literature_url': "",
                    'genetic_entity_external_url': "",
                    'mod_cross_reference_complete_url': ""}
            basic_genetic_entity = gene_record['basicGeneticEntity']
            primary_id = basic_genetic_entity.get('primaryId')
            global_id = basic_genetic_entity.get('primaryId')
            local_id = global_id.split(":")[1]
            taxon_id = basic_genetic_entity.get("taxonId")
            short_species_abbreviation = self.etlh.get_short_species_abbreviation(taxon_id)

            if basic_genetic_entity.get('taxonId') in ["NCBITaxon:9606", "NCBITaxon:10090"]:
                local_id = basic_genetic_entity.get('primaryId')

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(primary_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            self.xref_process(basic_genetic_entity, cross_references, urls, data_provider)
            # TODO Metadata can be safely removed from this dictionary. Needs to be tested.

            gene_to_so_terms.append({
                "primaryKey": primary_id,
                "soTermId": gene_record['soTermId']})

            gene = {
                "symbol": gene_record.get('symbol'),
                "symbolWithSpecies": gene_record.get('symbol') + " (" + short_species_abbreviation + ")",
                "name": gene_record.get('name'),
                "geneticEntityExternalUrl": urls['genetic_entity_external_url'],
                "description": gene_record.get('description'),
                "geneSynopsis": gene_record.get('geneSynopsis'),
                "geneSynopsisUrl": gene_record.get('geneSynopsisUrl'),
                "taxonId": basic_genetic_entity.get('taxonId'),
                "geneLiteratureUrl": urls['gene_literature_url'],
                "name_key": gene_record.get('symbol'),
                "primaryId": primary_id,
                "category": "gene",
                "href": None,
                "uuid": str(uuid.uuid4()),
                "modCrossRefCompleteUrl": urls['mod_cross_reference_complete_url'],
                "localId": local_id,
                "modGlobalCrossRefId": global_id,
                "modGlobalId": global_id,
                "loadKey": load_key,
                "dataProvider": data_provider,
                "dateProduced": date_produced}

            gene_dataset.append(gene)
            self.locations_process(basic_genetic_entity, chromosomes, genomic_locations)
            self.synonyms_process(synonyms, basic_genetic_entity)
            self.secondary_process(secondary_ids, basic_genetic_entity)

            # We should have the metadata ready to go after the first loop of the generator.
            self.metadata_is_loaded[load_key] = True

            # Establishes the number of genes to yield (return) at a time.
            if counter == batch_size:
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
