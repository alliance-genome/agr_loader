from contextlib import ExitStack
import csv
import logging

from .neo4j_transactor import Neo4jTransactor

logger = logging.getLogger(__name__)


class CSVTransactor(object):

    @staticmethod
    def save_file_static(generator, query_list_with_params):

        # generators = list of yielded lists from parser
        # query_list_with_parms = list of queries, each with batch size and CSV file name.

        for query_params in query_list_with_params:
            cypher_query_template = query_params.pop(0)  # Remove the first query + batch size + CSV file name
            #  from the list. Format the query with all remaining paramenters.
            query_to_run = cypher_query_template % tuple(query_params)
            while len(query_params) > 2:  # We need to remove extra params before we append
                # the modified query. Assuming the last entry in the list is the filepath
                query_params.pop()

            query_params.append(query_to_run)

        with ExitStack() as stack:
            # Open all necessary CSV files at once.
            open_files = [stack.enter_context(open('tmp/' + query_params[1], 'w', encoding='utf-8'))
                          for query_params in query_list_with_params]
            
            csv_file_writer = [None] * len(open_files)  # Create a list with 'None' placeholder entries.

            for generator_entry in generator:
                for index, individual_list in enumerate(generator_entry):
                    current_filename = open_files[index].name  # Our current CSV output file.
                    
                    # Remove None's from list which cause the write rows to crash
                    individual_list = [x for x in individual_list if x is not None]

                    if len(individual_list) == 0:
                        logger.debug("No data found when writing to csv! Skipping output file: %s" % current_filename)
                        continue

                    if csv_file_writer[index] is None:  # If we haven't yet created a DictWriter
                        # for this particular file.
                        try:
                            csv_file_writer[index] = csv.DictWriter(open_files[index],
                                                                    fieldnames=list(individual_list[0]),
                                                                    quoting=csv.QUOTE_ALL)
                            csv_file_writer[index].writeheader()  # Write the headers.
                        except Exception as e:
                            logger.critical("Couldn't write to file: %s " % current_filename)
                            logger.critical(e)

                    csv_file_writer[index].writerows(individual_list)  # Write the remainder of the list
                    # content for this iteration.
                    # logger.info("%s: Finished Writting %s entries to file: %s" % (self._get_name(),
                    # len(individual_list), current_filename))

        query_batch = []

        for query_param in query_list_with_params:
            query_batch.append([query_param[2], query_param[1]])  # neo4j query and filename.
        Neo4jTransactor.execute_query_batch(query_batch)