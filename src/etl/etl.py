"""ETL."""

import logging
import multiprocessing
import sys
import time

from test import TestObject
from etl.helpers import ETLHelper
from loader_common import ContextInfo


class ETL:
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

    def process_sub_types_with_threading(self, sub_type_objects, process_func,
                                         etl_type_name, cleanup_func=None):
        """
        Process sub-types either sequentially or in parallel based on configuration.

        This method abstracts the decision to run ETL sub-types sequentially or in parallel,
        based on whether the ETL type name appears in the SEQUENTIAL_ETL_TYPES environment
        variable / config setting.

        Sequential mode is useful for:
        - Preventing Neo4j deadlocks when multiple MODs update shared nodes (e.g., Gene)
        - Debugging and troubleshooting data loading issues
        - Reducing memory pressure on resource-constrained systems

        Args:
            sub_type_objects: List of sub-type config objects from
                             data_type_config.get_sub_type_objects()
            process_func: Function to call for each sub-type. Will be called as
                         process_func(sub_type) for each sub_type in sub_type_objects.
                         In parallel mode, this becomes the target for multiprocessing.Process.
            etl_type_name: The ETL type name to check against SEQUENTIAL_ETL_TYPES config.
                          Should match the key in aggregate_loader.py's etl_dispatch
                          (e.g., 'PHENOTYPE', 'DAF', 'ORTHO', 'GAF').
            cleanup_func: Optional function to call after processing. In sequential mode,
                         this is called after EACH sub-type. In parallel mode, this is
                         called ONCE after all sub-types complete. This is important for
                         DiseaseETL which needs to run delete_empty_nodes() after each
                         sub-type in sequential mode for correct data cleanup.

        Returns:
            None

        Configuration:
            Set SEQUENTIAL_ETL_TYPES environment variable or config to a comma-separated
            list of ETL type names:

            # Run phenotype and disease ETLs sequentially:
            export SEQUENTIAL_ETL_TYPES="PHENOTYPE,DAF"

            # Run all listed ETLs sequentially:
            export SEQUENTIAL_ETL_TYPES="PHENOTYPE,DAF,ORTHO,GAF"
        """
        context_info = ContextInfo()
        sequential_types_str = context_info.env.get("SEQUENTIAL_ETL_TYPES", "")

        # Parse comma-separated string into list, normalize to uppercase
        if sequential_types_str and sequential_types_str.strip():
            sequential_types = [t.strip().upper() for t in sequential_types_str.split(',') if t.strip()]
        else:
            sequential_types = []

        # Check if this ETL should run sequentially
        run_sequentially = etl_type_name.upper() in sequential_types

        if run_sequentially:
            self.logger.info("Loading %s sub-types SEQUENTIALLY (configured for deadlock prevention)",
                             etl_type_name)

            # Sequential execution: process each sub-type one at a time
            for sub_type in sub_type_objects:
                self.logger.info("Processing sub-type sequentially: %s", sub_type.get_data_provider())
                process_func(sub_type)

                # Run cleanup after EACH sub-type in sequential mode
                if cleanup_func is not None:
                    self.logger.debug("Running cleanup after sub-type: %s", sub_type.get_data_provider())
                    cleanup_func()

            self.logger.info("Completed %s sequential loading (%d sub-types)",
                             etl_type_name, len(sub_type_objects))
        else:
            self.logger.info("Loading %s sub-types in PARALLEL (%d sub-types)",
                             etl_type_name, len(sub_type_objects))

            # Parallel execution: spawn process for each sub-type
            thread_pool = []
            for sub_type in sub_type_objects:
                process = multiprocessing.Process(target=process_func, args=(sub_type,))
                process.start()
                thread_pool.append(process)

            ETL.wait_for_threads(thread_pool)

            # Run cleanup ONCE after all parallel processes complete
            if cleanup_func is not None:
                self.logger.debug("Running cleanup after parallel processing")
                cleanup_func()

            self.logger.info("Completed %s parallel loading", etl_type_name)

    def process_query_params(self, query_list_with_params):
        """Process Query Params."""
        # generators = list of yielded lists from parser
        # query_list_with_parms = list of queries, each with batch size and CSV file name.
        query_and_file_names = []

        # Example query_params:
        # [self.gene_synonyms_query_template, "gene_synonyms_" + sub_type.get_data_provider() + ".csv", commit_size]
        # [self.insert_isa_partof_closure_query_template, "isa_partof_closure_" + data_provider + ".csv", "100000", data_provider]

        for query_params in query_list_with_params:
            # Remove the first item (the template) from the list. Format that query with all remaining paramenters.
            cypher_query_template = query_params.pop(0)

            query_to_run = cypher_query_template % tuple(query_params)

            # while len(query_params) > 2:  # We need to remove extra params before we append
            #     # the modified query. Assuming the last entry in the list is the filepath
            #     query_params.pop()

            # The first item in query_params (after we've pop'ed an item above) should be the filepath.
            file_name = query_params[0]
            
            query_and_file_names.append([query_to_run, file_name])

        return query_and_file_names

    def data_providers_process(self, data):
        """Get data providers.

        Creates 4 attributes.
        data_provider: provider name/symbol
        data_providers: list of providers
        data_provider_pages: pages
        data_provider_cross_ref_set: list of xref dicts
        """
        data_provider_object = data['metaData']['dataProvider']

        data_provider_cross_ref = data_provider_object.get('crossReference')
        self.data_provider = data_provider_cross_ref.get('id')
        # Temporary Xenbase fix. 
        # Please remove when Xenbase has updated their BGI metadata ID to "Xenbase" instead of "XB".
        if self.data_provider == "XB":
            self.data_provider = "Xenbase"
        self.data_provider_pages = data_provider_cross_ref.get('pages')

        self.data_providers = []
        self.data_provider_cross_ref_set = []

        if self.data_provider_pages is None:
            return
        for data_provider_page in self.data_provider_pages:
            cross_ref_complete_url = self.etlh.rdh2.return_url_from_key_value(
                self.data_provider, self.data_provider, alt_page=data_provider_page)
            self.data_provider_cross_ref_set.append(
                ETLHelper.get_xref_dict(
                    self.data_provider,
                    self.data_provider,
                    data_provider_page,
                    data_provider_page,
                    self.data_provider,
                    cross_ref_complete_url,
                    self.data_provider + data_provider_page))

            self.data_providers.append(self.data_provider)
            self.logger.info("data provider: %s", self.data_provider)

    def ortho_xrefs(self, o_xrefs, ident, xrefs):
        """Generate xref for orthos."""
        if o_xrefs is None:
            return
        # turn into a list
        if type(o_xrefs) != list:
            self.logger.critical("o_xrefs is not a list but is a '{}'".format(type(o_xrefs)))
        for xref_id_dict in o_xrefs:
            xref_id = xref_id_dict["val"]
            if ":" in xref_id:
                local_id = xref_id.split(":")[1].strip()
                prefix = xref_id.split(":")[0].strip()
                complete_url = self.etlh.get_complete_url_ont(local_id, xref_id)
                generated_xref = ETLHelper.get_xref_dict(
                    local_id,
                    prefix,
                    "ontology_provided_cross_reference",
                    "ontology_provided_cross_reference",
                    xref_id,
                    complete_url,
                    xref_id + "ontology_provided_cross_reference")
                generated_xref["oid"] = ident
                xrefs.append(generated_xref)
        if ":" in o_xrefs:  # if o_xrefs is a str with ":" in it.
            local_id = o_xrefs.split(":")[1].strip()
            prefix = o_xrefs.split(":")[0].strip()
            complete_url = self.etlh.get_complete_url_ont(local_id, o_xrefs)
            generated_xref = ETLHelper.get_xref_dict(
                local_id,
                prefix,
                "ontology_provided_cross_reference",
                "ontology_provided_cross_reference",
                o_xrefs,
                complete_url,
                o_xrefs)
            generated_xref["oid"] = ident
            xrefs.append(generated_xref)
