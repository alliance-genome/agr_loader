'''Allele ETL'''

import logging
import multiprocessing
import uuid

from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import TextProcessingHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor


class AlleleETL(ETL):
    '''Allele ETL'''

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    allele_construct_no_gene_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (c:Construct {primaryKey: row.constructId})
            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (o:Allele {primaryKey:row.primaryId})
                ON CREATE SET o.symbol = row.symbol,
                 o.taxonId = row.taxonId,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.symbolText = row.symbolText,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider,
                 o.symbolWithSpecies = row.symbolWithSpecies,
                 o.symbolTextWithSpecies = row.symbolTextWithSpecies,
                 o.description = row.alleleDescription

            MERGE (o)-[:FROM_SPECIES]-(s)

            MERGE (o)-[:CONTAINS]-(c) """

    allele_construct_gene_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey: row.geneId})
            MATCH (c:Construct {primaryKey: row.constructId})
            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (o:Allele:Feature {primaryKey:row.primaryId})
                ON CREATE SET o.symbol = row.symbol,
                 o.taxonId = row.taxonId,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.symbolText = row.symbolText,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider,
                 o.symbolWithSpecies = row.symbolWithSpecies,
                 o.symbolTextWithSpecies = row.symbolTextWithSpecies,
                 o.description = row.alleleDescription

            MERGE (o)-[:FROM_SPECIES]-(s)
            MERGE (o)-[:IS_ALLELE_OF]-(g)
            MERGE (o)-[:CONTAINS]-(c) """


    allele_gene_no_construct_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey: row.geneId})
            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (o:Allele:Feature {primaryKey:row.primaryId})
                ON CREATE SET o.symbol = row.symbol,
                 o.taxonId = row.taxonId,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.symbolText = row.symbolText,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider,
                 o.symbolWithSpecies = row.symbolWithSpecies,
                 o.symbolTextWithSpecies = row.symbolTextWithSpecies,
                 o.description = row.alleleDescription

            MERGE (o)-[:FROM_SPECIES]-(s)

            MERGE (o)-[:IS_ALLELE_OF]->(g) """

    allele_no_gene_no_construct_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (s:Species {primaryKey: row.taxonId})

            //Create the Allele node and set properties. primaryKey is required.
            MERGE (o:Allele:Feature {primaryKey:row.primaryId})
                ON CREATE SET o.symbol = row.symbol,
                 o.taxonId = row.taxonId,
                 o.dateProduced = row.dateProduced,
                 o.release = row.release,
                 o.localId = row.localId,
                 o.globalId = row.globalId,
                 o.uuid = row.uuid,
                 o.symbolText = row.symbolText,
                 o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                 o.dataProviders = row.dataProviders,
                 o.dataProvider = row.dataProvider,
                 o.symbolWithSpecies = row.symbolWithSpecies,
                 o.symbolTextWithSpecies = row.symbolTextWithSpecies,
                 o.description = row.alleleDescription

            MERGE (o)-[:FROM_SPECIES]-(s)
    """

    allele_secondary_ids_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (f:Allele:Feature {primaryKey:row.data_id})

            MERGE (second:SecondaryId {primaryKey:row.secondary_id})
                SET second.name = row.secondary_id
            MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second) """

    allele_synonyms_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (a:Allele:Feature {primaryKey:row.data_id})

            MERGE(syn:Synonym {primaryKey:row.synonym})
                SET syn.name = row.synonym
            MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn) """

    allele_xrefs_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:Allele {primaryKey:row.dataId})""" + ETLHelper.get_cypher_xref_text()


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config


    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)


    def _process_sub_type(self, sub_type):
        '''processing subtype'''

        self.logger.info("Loading Allele Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        self.logger.info(filepath)
        data = JSONFile().get_data(filepath)
        self.logger.info("Finished Loading Allele Data: %s", sub_type.get_data_provider())

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.allele_gene_no_construct_query_template, commit_size,
             "allele_gene_no_construct_data_" + sub_type.get_data_provider() + ".csv"],
            [self.allele_construct_gene_query_template, commit_size,
             "allele_construct_gene_data_" + sub_type.get_data_provider() + ".csv"],
            [self.allele_construct_no_gene_query_template, commit_size,
             "allele_construct_no_gene_data_" + sub_type.get_data_provider() + ".csv"],
            [self.allele_no_gene_no_construct_query_template, commit_size,
             "allele_no_gene_no_construct_data_" + sub_type.get_data_provider() + ".csv"],
            [self.allele_secondary_ids_query_template, commit_size,
             "allele_secondary_ids_" + sub_type.get_data_provider() + ".csv"],
            [self.allele_synonyms_query_template, commit_size,
             "allele_synonyms_" + sub_type.get_data_provider() + ".csv"],
            [self.allele_xrefs_query_template, commit_size,
             "allele_xrefs_" + sub_type.get_data_provider() + ".csv"],
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, allele_data, data_provider, batch_size):
        '''Create Generators'''

        data_providers = []
        release = ""
        alleles_no_constrcut_no_gene = []
        alleles_construct_gene = []
        alleles_no_construct = []
        alleles_no_gene = []
        allele_synonyms = []
        allele_secondary_ids = []
        cross_reference_list = []

        counter = 0
        date_produced = allele_data['metaData']['dateProduced']

        data_provider_object = allele_data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        data_provider = data_provider_cross_ref.get('id')
        data_provider_pages = data_provider_cross_ref.get('pages')
        data_provider_cross_ref_set = []

        load_key = date_produced + data_provider + "_ALLELE"

        #TODO: get SGD to fix their files.

        if data_provider_pages is not None:
            for data_provider_page in data_provider_pages:
                cross_ref_complete_url = ETLHelper.get_page_complete_url(data_provider,
                                                                         self.xref_url_map,
                                                                         data_provider,
                                                                         data_provider_page)

                data_provider_cross_ref_set.append(ETLHelper.get_xref_dict(data_provider,
                        data_provider,
                        data_provider_page,
                        data_provider_page,
                        data_provider,
                        cross_ref_complete_url,
                        data_provider + data_provider_page))

                data_providers.append(data_provider)
                self.logger.info("data provider: %s", data_provider)

        if 'release' in allele_data['metaData']:
            release = allele_data['metaData']['release']

        for allele_record in allele_data['data']:
            counter = counter + 1
            global_id = allele_record['primaryId']
            # fixing parsing error on this end while MGI fixes on their end.
            if global_id == 'MGI:3826848':
                description = allele_record.get('description')[:-2]
            else:
                description = allele_record.get('description')

            local_id = global_id.split(":")[1]
            mod_global_cross_ref_id = ""

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            short_species_abbreviation = ETLHelper.get_short_species_abbreviation( \
                    allele_record.get('taxonId'))
            symbol_text = TextProcessingHelper.cleanhtml(allele_record.get('symbol'))

            gene = allele_record.get('gene')
            construct = allele_record.get('construct')

            if gene is not None and construct is not None:
                allele_construct_gene_dataset = {
                    "symbol": allele_record.get('symbol'),
                    "geneId": allele_record.get('gene'),
                    "primaryId": allele_record.get('primaryId'),
                    "globalId": global_id,
                    "localId": local_id,
                    "taxonId": allele_record.get('taxonId'),
                    "dataProviders": data_providers,
                    "dateProduced": date_produced,
                    "loadKey": load_key,
                    "release": release,
                    "modGlobalCrossRefId": mod_global_cross_ref_id,
                    "uuid": str(uuid.uuid4()),
                    "dataProvider": data_provider,
                    "symbolWithSpecies": allele_record.get('symbol')\
                                         + " ("+ short_species_abbreviation + ")",
                    "symbolTextWithSpecies": symbol_text + " ("+ short_species_abbreviation + ")",
                    "symbolText": symbol_text,
                    "alleleDescription": description,
                    "constructId": allele_record.get('construct')
                }
                alleles_construct_gene.append(allele_construct_gene_dataset)

            elif construct is not None and gene is None:
                allele_construct_no_gene_dataset = {
                    "symbol": allele_record.get('symbol'),
                    "primaryId": allele_record.get('primaryId'),
                    "globalId": global_id,
                    "localId": local_id,
                    "taxonId": allele_record.get('taxonId'),
                    "dataProviders": data_providers,
                    "dateProduced": date_produced,
                    "loadKey": load_key,
                    "release": release,
                    "modGlobalCrossRefId": mod_global_cross_ref_id,
                    "uuid": str(uuid.uuid4()),
                    "dataProvider": data_provider,
                    "symbolWithSpecies": allele_record.get('symbol') \
                                         + " (" + short_species_abbreviation + ")",
                    "symbolTextWithSpecies": symbol_text + " ("+ short_species_abbreviation + ")",
                    "symbolText": symbol_text,
                    "alleleDescription": description,
                    "constructId": allele_record.get('construct')
                }

                alleles_no_gene.append(allele_construct_no_gene_dataset)

            elif gene is not None and construct is None:
                allele_gene_no_construct_dataset = {
                    "symbol": allele_record.get('symbol'),
                    "geneId": allele_record.get('gene'),
                    "primaryId": allele_record.get('primaryId'),
                    "globalId": global_id,
                    "localId": local_id,
                    "taxonId": allele_record.get('taxonId'),
                    "dataProviders": data_providers,
                    "dateProduced": date_produced,
                    "loadKey": load_key,
                    "release": release,
                    "modGlobalCrossRefId": mod_global_cross_ref_id,
                    "uuid": str(uuid.uuid4()),
                    "dataProvider": data_provider,
                    "symbolWithSpecies": allele_record.get('symbol') + " (" + short_species_abbreviation + ")",
                    "symbolTextWithSpecies": symbol_text + " ("+ short_species_abbreviation + ")",
                    "symbolText": symbol_text,
                    "alleleDescription": description
                }

                alleles_no_construct.append(allele_gene_no_construct_dataset)

            elif gene is None and construct is None:
                allele_no_gene_no_construct_dataset = {
                    "symbol": allele_record.get('symbol'),
                    "primaryId": allele_record.get('primaryId'),
                    "globalId": global_id,
                    "localId": local_id,
                    "taxonId": allele_record.get('taxonId'),
                    "dataProviders": data_providers,
                    "dateProduced": date_produced,
                    "loadKey": load_key,
                    "release": release,
                    "modGlobalCrossRefId": mod_global_cross_ref_id,
                    "uuid": str(uuid.uuid4()),
                    "dataProvider": data_provider,
                    "symbolWithSpecies": allele_record.get('symbol') \
                                         + " ("+ short_species_abbreviation + ")",
                    "symbolTextWithSpecies": symbol_text + " ("+ short_species_abbreviation + ")",
                    "symbolText": symbol_text,
                    "alleleDescription": description
                }

                alleles_no_constrcut_no_gene.append(allele_no_gene_no_construct_dataset)

            else:
                self.logger.debug("ERROR: missing construct and gene")


            if 'crossReferences' in allele_record:

                for cross_ref in allele_record['crossReferences']:
                    cross_ref_id = cross_ref.get('id')
                    local_crossref_id = cross_ref_id.split(":")[1]
                    prefix = cross_ref.get('id').split(":")[0]
                    pages = cross_ref.get('pages')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            if page in ['allele', 'allele/references', 'transgene',
                                        'construct', 'transgene/references',
                                        'construct/references']:
                                mod_global_cross_ref_id = ETLHelper.get_page_complete_url(local_crossref_id,
                                        self.xref_url_map,
                                        prefix,
                                        page)
                                xref = ETLHelper.get_xref_dict(local_crossref_id,
                                                               prefix,
                                                               page,
                                                               page,
                                                               cross_ref_id,
                                                               mod_global_cross_ref_id,
                                                               cross_ref_id + page)
                                xref['dataId'] = global_id
                                cross_reference_list.append(xref)

            if 'synonyms' in allele_record:
                for syn in allele_record.get('synonyms'):
                    allele_synonym = {
                        "data_id": allele_record.get('primaryId'),
                        "synonym": syn.strip()
                    }
                    allele_synonyms.append(allele_synonym)

            if 'secondaryIds' in allele_record:
                for secondary_id in allele_record.get('secondaryIds'):
                    allele_secondary_id = {
                        "data_id": allele_record.get('primaryId'),
                        "secondary_id": secondary_id
                    }
                    allele_secondary_ids.append(allele_secondary_id)

            if counter == batch_size:
                yield [alleles_no_construct,
                       alleles_construct_gene,
                       alleles_no_gene,
                       alleles_no_constrcut_no_gene,
                       allele_secondary_ids,
                       allele_synonyms,
                       cross_reference_list]
                alleles_no_construct = []
                alleles_construct_gene = []
                alleles_no_gene = []
                alleles_no_constrcut_no_gene = []

                allele_secondary_ids = []
                allele_synonyms = []
                cross_reference_list = []
                counter = 0

        if counter > 0:
            yield [alleles_no_construct,
                   alleles_construct_gene,
                   alleles_no_gene,
                   alleles_no_constrcut_no_gene,
                   allele_secondary_ids,
                   allele_synonyms,
                   cross_reference_list]
