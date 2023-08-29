"""Paralogy ETL."""

from itertools import permutations
import logging
import uuid
import multiprocessing
import codecs
from random import shuffle
import ijson

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
# from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor


class ParalogyETL(ETL):
    """Paralogy ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    main_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                //using match here to limit paralog set to genes that have already been loaded by bgi.
                MATCH(g1:Gene {primaryKey:row.gene1AgrPrimaryId})
                MATCH(g2:Gene {primaryKey:row.gene2AgrPrimaryId})

                CREATE (g1)-[para:PARALOGOUS]->(g2)
                    SET para.primaryKey = row.uuid,
                        para.confidence = row.confidence,
                        para.length = row.length,
                        para.similarity = row.similarity,
                        para.identity = row.identity,
                        para.rank = row.rank

                //Create the Association node to be used for the object/doTerm
                CREATE (oa:Association {primaryKey:row.uuid})
                    SET oa.joinType = 'paralogous'
                    SET oa :ParalogyGeneJoin
                CREATE (g1)-[a1:ASSOCIATION]->(oa)
                CREATE (oa)-[a2:ASSOCIATION]->(g2)
            }
        IN TRANSACTIONS of %s ROWS"""

    not_matched_algorithm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ogj:ParalogyGeneJoin {primaryKey:row.uuid})
                MATCH (oa:ParaAlgorithm {name:row.algorithm})
                CREATE (ogj)-[:NOT_MATCHED]->(oa)
            }
        IN TRANSACTIONS of %s ROWS"""

    matched_algorithm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ogj:ParalogyGeneJoin {primaryKey:row.uuid})
                MATCH (oa:ParaAlgorithm {name:row.algorithm})
                CREATE (ogj)-[:MATCHED]->(oa)
            }
        IN TRANSACTIONS of %s ROWS"""

    not_called_algorithm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ogj:ParalogyGeneJoin {primaryKey:row.uuid})
                MATCH (oa:ParaAlgorithm {name:row.algorithm})
                CREATE (ogj)-[:NOT_CALLED]->(oa)
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        self.load_algorithm = """
            CREATE (oa:ParaAlgorithm)
                SET oa.name = $parameter
        """

        # Load single pass of algorithms in first.
        list_of_algorithms = ["Ensembl Compara", 
                            "HGNC", 
                            "Hieranoid", 
                            "InParanoid", 
                            "OMA", 
                            "OrthoFinder", 
                            "OrthoInspector", 
                            "PANTHER", 
                            "PhylomeDB", 
                            "SonicParanoid",
                            "SGD"]

        for algorithm in list_of_algorithms:
            self.logger.info("Loading algorithm node: %s", algorithm)
            with Neo4jHelper.run_single_parameter_query(self.load_algorithm, algorithm) as results:
                for result in results:
                    self.logger.debug(result)

        thread_pool = []

        query_tracking_list = multiprocessing.Manager().list()
        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type,
                                              args=(sub_type,
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

        Neo4jTransactor.execute_query_batch(algo_queries)
        self.error_messages()

        Neo4jTransactor.execute_query_batch(queries)
        self.error_messages()

    def _process_sub_type(self, sub_type, query_tracking_list):
        self.logger.info("Loading Paralogy Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        # data = JSONFile().get_data(filepath)

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(filepath,
                                         sub_type.get_data_provider(),
                                         batch_size)

        query_template_list = []

        query_template_list.append(
            [self.main_query_template,
            "Paralogy_data_" + sub_type.get_data_provider() + ".csv", "100000"])
        query_template_list.append(
            [self.matched_algorithm_query_template, 
             "Paralogy_matched_algorithm_data_{}.csv".format(sub_type.get_data_provider()), commit_size])
        query_template_list.append(
            [self.not_matched_algorithm_query_template,
             "Paralogy_not_matched_algorithm_data_" + sub_type.get_data_provider() + ".csv", commit_size])
        query_template_list.append(
            [self.not_called_algorithm_query_template,
             "Paralogy_not_called_algorithm_data_" + sub_type.get_data_provider() + ".csv", commit_size])

        query_and_file_list = self.process_query_params(query_template_list)

        CSVTransactor.save_file_static(generators, query_and_file_list)

        for item in query_and_file_list:
            query_tracking_list.append(item)

        self.error_messages("Paralogy-{}: ".format(sub_type.get_data_provider()))
        self.logger.info("Finished Loading Paralogy Data: %s", sub_type.get_data_provider())

    def safely_round(value):
        # Check if value is of type float or int
        if isinstance(value, (float, int)):
            return round(value, 2)
        else:
            return value

    def get_generators(self, datafile, sub_type, batch_size):  # noqa
        """Get Generators."""
        counter = 0

        matched_algorithm_data = []
        unmatched_algorithm_data = []
        not_called_algorithm_data = []
        paralogy_data = []

        self.logger.info("streaming json data from %s ...", datafile)
        with codecs.open(datafile, 'r', 'utf-8') as file_handle:

            for para_record in ijson.items(file_handle, 'data.item'):
                # Sort out identifiers and prefixes.
                gene_1 = ETLHelper.process_identifiers(para_record['gene1'])
                # 'DRSC:'' removed, local ID, functions as display ID.
                gene_2 = ETLHelper.process_identifiers(para_record['gene2'])
                # 'DRSC:'' removed, local ID, functions as display ID.

                gene_1_species_taxon_id = para_record['species']
                gene_2_species_taxon_id = para_record['species']

                # Prefixed according to AGR prefixes.
                gene_1_agr_primary_id = ETLHelper.add_agr_prefix_by_species_taxon(
                    gene_1, gene_1_species_taxon_id)

                # Prefixed according to AGR prefixes.
                gene_2_agr_primary_id = ETLHelper.add_agr_prefix_by_species_taxon(
                    gene_2, gene_2_species_taxon_id)

                counter = counter + 1

                para_uuid = str(uuid.uuid4())

                if self.test_object.using_test_data() is True:
                    is_it_test_entry = self.test_object.check_for_test_id_entry(gene_1_agr_primary_id)
                    is_it_test_entry_2 = self.test_object.check_for_test_id_entry(gene_2_agr_primary_id)
                    if is_it_test_entry is False and is_it_test_entry_2 is False:
                        counter = counter - 1
                        continue

                # Obtain similarity from dictionary and round to two decimal places.
                similarity = safely_round(para_record['similarity'])
                identity = safely_round(para_record['identity'])

                if gene_1_agr_primary_id is not None and gene_2_agr_primary_id is not None:

                    para_dataset = {
                        'rank': para_record['rank'],
                        'length': para_record['length'],
                        'similarity': similarity,
                        'identity': identity,

                        'gene1AgrPrimaryId': gene_1_agr_primary_id,
                        'gene2AgrPrimaryId': gene_2_agr_primary_id,

                        'confidence': para_record['confidence'],

                        'uuid': para_uuid
                    }
                    paralogy_data.append(para_dataset)

                    for matched in para_record.get('predictionMethodsMatched'):
                        matched_dataset = {
                            "uuid": para_uuid,
                            "algorithm": matched
                        }
                        matched_algorithm_data.append(matched_dataset)

                    for unmatched in para_record.get('predictionMethodsNotMatched'):
                        unmatched_dataset = {
                            "uuid": para_uuid,
                            "algorithm": unmatched
                        }
                        unmatched_algorithm_data.append(unmatched_dataset)

                    for not_called in para_record.get('predictionMethodsNotCalled'):
                        not_called_dataset = {
                            "uuid": para_uuid,
                            "algorithm": not_called
                        }
                        not_called_algorithm_data.append(not_called_dataset)

                    # Establishes the number of entries to yield (return) at a time.
                    if counter == batch_size:
                        list_to_yield = []
                        list_to_yield.append(paralogy_data)
                        list_to_yield.append(matched_algorithm_data)
                        list_to_yield.append(unmatched_algorithm_data)
                        list_to_yield.append(not_called_algorithm_data)

                        yield list_to_yield

                        paralogy_data = []
                        matched_algorithm_data = []
                        unmatched_algorithm_data = []
                        not_called_algorithm_data = []
                        counter = 0

            if counter > 0:
                list_to_yield = []
                list_to_yield.append(paralogy_data)
                list_to_yield.append(matched_algorithm_data)
                list_to_yield.append(unmatched_algorithm_data)
                list_to_yield.append(not_called_algorithm_data)

                yield list_to_yield
