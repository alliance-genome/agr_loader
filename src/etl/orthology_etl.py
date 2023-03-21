"""Orthology ETL."""

from itertools import permutations
import logging
import uuid
import multiprocessing
import codecs
from random import shuffle
import ijson

from etl import ETL
from etl.helpers import ETLHelper
# from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor


class OrthologyETL(ETL):
    """Orthology ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    main_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                //using match here to limit ortho set to genes that have already been loaded by bgi.
                MATCH(g1:Gene {primaryKey:row.gene1AgrPrimaryId})
                MATCH(g2:Gene {primaryKey:row.gene2AgrPrimaryId})

                CREATE (g1)-[orth:ORTHOLOGOUS]->(g2)
                    SET orth.primaryKey = row.uuid,
                        orth.isBestScore = row.isBestScore,
                        orth.isBestRevScore = row.isBestRevScore,
                        orth.confidence = row.confidence,
                        orth.strictFilter = toBoolean(row.strictFilter),
                        orth.moderateFilter = toBoolean(row.moderateFilter)

                //Create the Association node to be used for the object/doTerm
                CREATE (oa:Association:OrthologyGeneJoin {primaryKey:row.uuid})
                    SET oa.joinType = 'orthologous'
                CREATE (g1)-[a1:ASSOCIATION]->(oa)
                CREATE (oa)-[a2:ASSOCIATION]->(g2)
            }
        IN TRANSACTIONS of %s ROWS"""

    not_matched_algorithm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ogj:OrthologyGeneJoin {primaryKey:row.uuid})
                MERGE (oa:OrthoAlgorithm {name:row.algorithm})
                CREATE (ogj)-[:NOT_MATCHED]->(oa)
            }
        IN TRANSACTIONS of %s ROWS"""

    matched_algorithm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ogj:OrthologyGeneJoin {primaryKey:row.uuid})
                MERGE (oa:OrthoAlgorithm {name:row.algorithm})
                CREATE (ogj)-[:MATCHED]->(oa)
            }
        IN TRANSACTIONS of %s ROWS"""

    not_called_algorithm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ogj:OrthologyGeneJoin {primaryKey:row.uuid})
                MERGE (oa:OrthoAlgorithm {name:row.algorithm})
                CREATE (ogj)-[:NOT_CALLED]->(oa)
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        sub_types = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            sub_types.append(sub_type.get_data_provider())

        thread_pool = []

        query_tracking_list = multiprocessing.Manager().list()
        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type,
                                              args=(sub_type, sub_types,
                                                    query_tracking_list))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

        queries = []
        for item in query_tracking_list:
            queries.append(item)

        algo_queries = []

        for item in queries:
            if "algorithm" in item[1]:
                algo_queries.append(item)

        main_list = self.get_randomized_list(sub_types)

        for file_set in main_list:
            for pair in file_set:
                for item in queries:
                    if pair[0] + "_" + pair[1] in item[1]:
                        self.logger.debug("Pair: %s Item: %s", pair, item[1])
                        Neo4jTransactor.execute_query_batch([item])

            Neo4jTransactor().wait_for_queues()

        Neo4jTransactor.execute_query_batch(algo_queries)
        self.error_messages()

    def _process_sub_type(self, sub_type, sub_types, query_tracking_list):
        self.logger.info("Loading Orthology Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        # data = JSONFile().get_data(filepath)

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(filepath,
                                         sub_type.get_data_provider(),
                                         sub_types,
                                         batch_size)

        query_template_list = []

        for mod_sub_type in sub_types:
            if mod_sub_type != sub_type.get_data_provider():
                query_template_list.append(
                    [self.main_query_template,
                     "orthology_data_" + sub_type.get_data_provider() + "_" + mod_sub_type + ".csv", "100000"])

        query_template_list.append(
            [self.matched_algorithm_query_template, 
             "orthology_matched_algorithm_data_{}.csv".format(sub_type.get_data_provider()), commit_size])
        query_template_list.append(
            [self.not_matched_algorithm_query_template,
             "orthology_not_matched_algorithm_data_" + sub_type.get_data_provider() + ".csv", commit_size])
        query_template_list.append(
            [self.not_called_algorithm_query_template,
             "orthology_not_called_algorithm_data_" + sub_type.get_data_provider() + ".csv", commit_size])

        query_and_file_list = self.process_query_params(query_template_list)

        CSVTransactor.save_file_static(generators, query_and_file_list)

        for item in query_and_file_list:
            query_tracking_list.append(item)

        self.error_messages("Ortho-{}: ".format(sub_type.get_data_provider()))
        self.logger.info("Finished Loading Orthology Data: %s", sub_type.get_data_provider())

    def get_randomized_list(self, sub_types):
        """Get Randomized List."""
        pairs = [perm for perm in permutations(sub_types, 2)]

        list_o = []
        counter = 0
        while (len(list_o) == 0 or len(list_o) > len(sub_types) * 2) and counter < 10000:

            list_o = []

            shuffle(pairs)
            for pair in pairs:
                inserted = False
                for item in list_o:
                    found = False
                    for item2 in item:
                        if pair[0] in item2 or pair[1] in item2:
                            found = True
                            break
                    if not found:
                        item.append(pair)
                        inserted = True
                        break
                if not inserted:
                    list_o.append([pair])

            counter += 1

        return list_o

    def get_generators(self, datafile, sub_type, sub_types, batch_size):  # noqa
        """Get Generators."""
        counter = 0

        matched_algorithm_data = []
        unmatched_algorithm_data = []
        not_called_algorithm_data = []

        list_of_mod_lists = {}

        for mod_sub_type in sub_types:
            if mod_sub_type != sub_type:
                list_of_mod_lists[mod_sub_type] = []

        self.logger.info("streaming json data from %s ...", datafile)
        with codecs.open(datafile, 'r', 'utf-8') as file_handle:

            for ortho_record in ijson.items(file_handle, 'data.item'):
                # Sort out identifiers and prefixes.
                gene_1 = ETLHelper.process_identifiers(ortho_record['gene1'])
                # 'DRSC:'' removed, local ID, functions as display ID.
                gene_2 = ETLHelper.process_identifiers(ortho_record['gene2'])
                # 'DRSC:'' removed, local ID, functions as display ID.

                gene_1_species_taxon_id = ortho_record['gene1Species']
                gene_2_species_taxon_id = ortho_record['gene2Species']

                # Prefixed according to AGR prefixes.
                gene_1_agr_primary_id = ETLHelper.add_agr_prefix_by_species_taxon(
                    gene_1, gene_1_species_taxon_id)

                # Prefixed according to AGR prefixes.
                gene_2_agr_primary_id = ETLHelper.add_agr_prefix_by_species_taxon(
                    gene_2, gene_2_species_taxon_id)
                gene_2_data_provider = self.etlh.get_mod_from_taxon(str(gene_2_species_taxon_id))

                counter = counter + 1

                ortho_uuid = str(uuid.uuid4())

                if self.test_object.using_test_data() is True:
                    is_it_test_entry = self.test_object.check_for_test_id_entry(gene_1_agr_primary_id)
                    is_it_test_entry_2 = self.test_object.check_for_test_id_entry(gene_2_agr_primary_id)
                    if is_it_test_entry is False and is_it_test_entry_2 is False:
                        counter = counter - 1
                        continue

                if gene_1_agr_primary_id is not None and gene_2_agr_primary_id is not None:

                    ortho_dataset = {
                        'isBestScore': ortho_record['isBestScore'],
                        'isBestRevScore': ortho_record['isBestRevScore'],

                        'gene1AgrPrimaryId': gene_1_agr_primary_id,
                        'gene2AgrPrimaryId': gene_2_agr_primary_id,

                        'confidence': ortho_record['confidence'],

                        'strictFilter': ortho_record['strictFilter'],
                        'moderateFilter': ortho_record['moderateFilter'],
                        'uuid': ortho_uuid
                    }
                    list_of_mod_lists[gene_2_data_provider].append(ortho_dataset)

                    for matched in ortho_record.get('predictionMethodsMatched'):
                        matched_dataset = {
                            "uuid": ortho_uuid,
                            "algorithm": matched
                        }
                        matched_algorithm_data.append(matched_dataset)

                    for unmatched in ortho_record.get('predictionMethodsNotMatched'):
                        unmatched_dataset = {
                            "uuid": ortho_uuid,
                            "algorithm": unmatched
                        }
                        unmatched_algorithm_data.append(unmatched_dataset)

                    for not_called in ortho_record.get('predictionMethodsNotCalled'):
                        not_called_dataset = {
                            "uuid": ortho_uuid,
                            "algorithm": not_called
                        }
                        not_called_algorithm_data.append(not_called_dataset)

                    # Establishes the number of entries to yield (return) at a time.
                    if counter == batch_size:
                        list_to_yield = []
                        for mod_sub_type in sub_types:
                            if mod_sub_type != sub_type:
                                list_to_yield.append(list_of_mod_lists[mod_sub_type])
                        list_to_yield.append(matched_algorithm_data)
                        list_to_yield.append(unmatched_algorithm_data)
                        list_to_yield.append(not_called_algorithm_data)

                        yield list_to_yield

                        for mod_sub_type in sub_types:
                            if mod_sub_type != sub_type:
                                list_of_mod_lists[mod_sub_type] = []
                        matched_algorithm_data = []
                        unmatched_algorithm_data = []
                        not_called_algorithm_data = []
                        counter = 0

            if counter > 0:
                list_to_yield = []
                for mod_sub_type in sub_types:
                    if mod_sub_type != sub_type:
                        list_to_yield.append(list_of_mod_lists[mod_sub_type])
                list_to_yield.append(matched_algorithm_data)
                list_to_yield.append(unmatched_algorithm_data)
                list_to_yield.append(not_called_algorithm_data)

                yield list_to_yield
