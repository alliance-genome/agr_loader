"""ETL."""

import logging
import sys
import time

from test import TestObject
from etl.helpers import ETLHelper
from loader_common import ContextInfo


class ETL():
    """ETL."""

    logger = logging.getLogger(__name__)
    etlh = ETLHelper()

    def __init__(self):
        """Initialise objects."""
        context_info = ContextInfo()
        self.schema_branch = context_info.env["TEST_SCHEMA_BRANCH"]

        if context_info.env["TEST_SET"]:
            self.logger.warning("WARNING: Test data load enabled.")
            time.sleep(1)
            self.test_object = TestObject(True)
        else:
            self.test_object = TestObject(False)

    def error_messages(self, prefix=""):
        """Print out error summary messages."""
        for key in self.etlh.rdh2.missing_pages.keys():
            self.logger.critical("%s Missing page %s seen %s times", prefix, key, self.etlh.rdh2.missing_pages[key])
        self.etlh.rdh2.missing_pages = {}
        for key in self.etlh.rdh2.missing_keys.keys():
            self.logger.critical("%s Missing key %s seen %s times", prefix, key, self.etlh.rdh2.missing_keys[key])
        self.etlh.rdh2.missing_keys = {}
        for key in self.etlh.rdh2.deprecated_mess.keys():
            self.logger.critical("%s Deprecated %s seen %s times", prefix, key, self.etlh.rdh2.deprecated_mess[key])
        self.etlh.rdh2.deprecated_mess = {}
        for key in self.etlh.rdh2.bad_pages.keys():
            self.logger.critical("%s None matching urls %s seen %s times", prefix, key, self.etlh.rdh2.bad_pages[key])
            self.etlh.rdh2.bad_pages = {}
        for key in self.etlh.rdh2.bad_regex.keys():
            self.logger.critical("%s None matching regex %s seen %s times", prefix, key, self.etlh.rdh2.bad_regex[key])
        self.etlh.rdh2.bad_regex = {}

    def run_etl(self):
        """Run ETL."""
        self._load_and_process_data()
        self.error_messages("ETL main:")

    @staticmethod
    def wait_for_threads(thread_pool, queue=None):
        """Wait for Threads."""
        ETL.logger.debug("Waiting for Threads to finish: %s", len(thread_pool))

        while len(thread_pool) > 0:
            ETL.logger.debug("Checking Threads: %s", len(thread_pool))
            for (index, thread) in enumerate(thread_pool):
                ETL.logger.debug("Thread Alive: %s Exitcode: %s",
                                 thread.is_alive(),
                                 thread.exitcode)
                if (thread.exitcode is None or thread.exitcode == 0) and not thread.is_alive():
                    ETL.logger.debug("Thread Finished Removing from pool: ")
                    thread.join()
                    del thread_pool[index]
                elif thread.exitcode is not None and thread.exitcode != 0:
                    ETL.logger.debug("Thread has Problems Killing Children: ")
                    for thread1 in thread_pool:
                        thread1.terminate()
                    sys.exit(-1)
                else:
                    pass

            if queue is not None:
                ETL.logger.debug("Queue Size: %s", queue.qsize())
                if queue.empty():
                    queue.join()
                    return

            time.sleep(5)

    def process_query_params(self, query_list_with_params):
        """Process Query Params."""
        # generators = list of yielded lists from parser
        # query_list_with_parms = list of queries, each with batch size and CSV file name.
        query_and_file_names = []

        for query_params in query_list_with_params:
            # Remove the first query + batch size + CSV file name
            cypher_query_template = query_params.pop(0)

            #  from the list. Format the query with all remaining paramenters.
            query_to_run = cypher_query_template % tuple(query_params)

            while len(query_params) > 2:  # We need to remove extra params before we append
                # the modified query. Assuming the last entry in the list is the filepath
                query_params.pop()

            file_name = query_params.pop()
            query_and_file_names.append([query_to_run, file_name])

        return query_and_file_names
