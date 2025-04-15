"""Neo4j Transacotr"""

import logging
import multiprocessing
import pickle
import time
# Import specific exceptions if needed for check below
from neo4j import GraphDatabase, exceptions as neo4j_exceptions
from etl import ETL
from loader_common import ContextInfo


class Neo4jTransactor():
    """Neo4j Transactor"""

    logger = logging.getLogger(__name__)
    count = 0
    queue = None

    def __init__(self):
        self.thread_pool = []


    @staticmethod
    def _get_name():
        return "Neo4jTransactor %s" % multiprocessing.current_process().name

    def start_threads(self, thread_count):
        """Start Threads"""

        manager = multiprocessing.Manager()
        queue = manager.Queue()
        Neo4jTransactor.queue = queue

        for i in range(0, thread_count):
            process = multiprocessing.Process(target=self.run, name=str(i))
            process.start()
            self.thread_pool.append(process)

    def shutdown(self):
        """Shutdown"""

        self.logger.info("Shutting down Neo4jTransactor threads: %s", len(self.thread_pool))
        for thread in self.thread_pool:
            thread.terminate()
        self.logger.info("Finished Shutting down Neo4jTransactor threads")

    @staticmethod
    def execute_query_batch(query_batch):
        """Execture Query Batch"""

        Neo4jTransactor.count = Neo4jTransactor.count + 1
        batch_id = Neo4jTransactor.count # Added batch_id variable
        queue_size = -1
        try: queue_size = Neo4jTransactor.queue.qsize()
        except NotImplementedError: pass

        # --- Create the stateful batch object --- # 
        batch_data = { # New variable/dict holding state
            'all_queries': list(query_batch), # Store original list
            'next_query_index': 0,            # Add starting index
            'retries': 0,                     # Add starting retry counter
            'batch_id': batch_id              # Add batch id
        }
        # --- End modification --- # 

        # Modified log to use batch_id and size from dict
        Neo4jTransactor.logger.debug("Adding Query Batch: %s BatchSize: %s QueueSize: %s ",
                                     batch_id, len(batch_data['all_queries']), queue_size)
        # Put the new state object (dict) and original counter id onto the queue
        Neo4jTransactor.queue.put((batch_data, batch_id)) # Modified item put on queue

    def check_for_thread_errors(self):
        """Check for Thread Errors"""

        ETL.wait_for_threads(self.thread_pool, Neo4jTransactor.queue)

    @staticmethod
    def wait_for_queues():
        """Wait for Queues"""
        # NOTE: This method remains unchanged from the original provided code.
        # It will raise a NameError if called because staticmethods cannot access self.logger.
        Neo4jTransactor.queue.join()

    def run(self):
        """Run"""

        context_info = ContextInfo()
        graph = None
        max_retries = 10 # Define max_retries needed for new logic

        if context_info.env["USING_PICKLE"] is False:
            # Keep original connection try/except structure - minimal change
            # Note: Original didn't explicitly handle connection errors here
            uri = "bolt://" + context_info.env["NEO4J_HOST"] + ":" + str(context_info.env["NEO4J_PORT"])
            graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1, fetch_size=10000)

        process_name = self._get_name() # Moved after graph init to match original structure closer
        self.logger.info("%s: Starting Neo4jTransactor Thread Runner: ", process_name)
        while True:
            item = None # Added variable for finally check
            batch_data = None # Added variable for state dict
            query_counter = None # Variable for original counter from queue

            try:
                # Get item from queue, expecting (batch_data_dict, batch_id_counter)
                item = Neo4jTransactor.queue.get() # Assign item
                (batch_data, query_counter) = item # Unpack item
            except EOFError as error:
                self.logger.info("Queue Closed exiting: %s", error)
                # No explicit graph.close() in original finally, keeping minimal
                return
            # Minimal error handling for queue get
            except Exception as e:
                 Neo4jTransactor.logger.error(f"{process_name}: Error getting item from queue: {e}. Worker stopping.", exc_info=True)
                 return

            # --- Extract state from batch_data --- # Added comment
            batch_id = batch_data['batch_id'] # Get batch_id from dict
            all_queries = batch_data['all_queries'] # Get full query list from dict

            # Original batch processing log - adapted slightly
            self.logger.debug("%s: Processing query batch: %s StartingIndex: %s TotalQueries: %s",
                              process_name, batch_id, batch_data['next_query_index'], len(all_queries)) # Use state vars
            batch_start = time.time()

            # total_query_counter = 0 # Removed original counter

            processed_this_attempt = 0 # Added counter for this attempt
            batch_requeued = False # Added flag

            # --- Inner loop iterates using the index --- # Added comment
            while batch_data['next_query_index'] < len(all_queries): # Loop based on index
                current_index = batch_data['next_query_index'] # Get current index
                # Get query based on index from the stored list
                (neo4j_query, filename) = all_queries[current_index] # Access query by index

                # Original query processing log - adapted for index
                queue_size = -1
                try: queue_size = Neo4jTransactor.queue.qsize()
                except NotImplementedError: pass
                self.logger.debug("%s: Processing query for file: %s QueryNum: %s Index: %s QueueSize: %s",
                                  process_name, filename, batch_id, current_index, queue_size) # Use state vars
                start = time.time()
                try:
                    # Original Pickle logic - adapted for index in filename
                    if context_info.env["USING_PICKLE"] is True:
                        pickle_dir = context_info.env.get("PICKLE_PATH", "tmp/temp") # Get path
                        # Original filename formatting used index/counter, adapt slightly
                        file_name = f"{pickle_dir}/transaction_{batch_id}_{current_index}.pkl" # Use index
                        with open(file_name, 'wb') as file:
                            self.logger.debug("Writing to file: %s", file_name) # Use f-string for path
                            pickle.dump(neo4j_query, file)
                    else:
                        # Original Neo4j execution logic
                        # Adding graph check for safety, minimal intrusion
                        if not graph: raise ConnectionError("Neo4j driver not initialized in worker.")
                        with graph.session() as session:
                            session.run(neo4j_query)

                    # --- Success --- #
                    end = time.time()
                    elapsed_time = end - start
                    # Original success log - adapted for index
                    self.logger.info("%s: Processed query for file: %s QueryNum: %s Index: %s QueueSize: %s Time: %s",
                                     process_name, filename, batch_id, current_index, queue_size,
                                     time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) # Use state vars
                    # --- IMPORTANT: Increment index and reset retries on success --- # 
                    batch_data['next_query_index'] += 1 # Increment index
                    batch_data['retries'] = 0 # Reset retries
                    processed_this_attempt += 1 # Increment attempt counter
                    # --- End Success --- # 

                except Exception as error:
                    # Original error log
                    self.logger.error(error)

                    # --- Check for Constraint Error --- # 
                    # Using original style check as much as possible
                    is_constraint_error = hasattr(error, 'code') and \
                                          isinstance(error, neo4j_exceptions.ClientError) and \
                                          'ConstraintValidationFailed' in error.code

                    if is_constraint_error: # Check if constraint error
                        # Original behavior: Log critical and stop worker by raising
                        self.logger.critical(
                            "%s: Constraint violation, aborting processing for file: %s. Worker stopping.",
                            process_name, filename) # Original log
                        raise error # Stop worker by re-raising

                    # --- Handle Other Errors (Treat as Transient for Retry) --- # 
                    else:
                        batch_data['retries'] += 1 # Increment retries counter in batch state
                        # Original warning log - adapted for new retry info
                        self.logger.warning(
                            "%s: Query Conflict, putting data back in Queue to run later. File: %s (Retry %d/%d)",
                            process_name, filename, batch_data['retries'], max_retries) # Modified log

                        if batch_data['retries'] > max_retries: # Check max retries
                            # Original behavior: Log error and stop worker by raising
                            self.logger.error(
                                "%s: Max retries exceeded for file: %s at Index %s. Raising error.",
                                process_name, filename, current_index) # Modified log
                            raise RuntimeError(f"Max retries exceeded for {filename}") # Stop worker

                        else: # If okay to retry
                            # --- Requeue the batch_data with current state --- 
                            try: # Minimal try/except for queue put safety
                                # Put the state dict back, index is NOT incremented
                                Neo4jTransactor.queue.put((batch_data, query_counter))
                                batch_requeued = True # Set flag
                                time.sleep(12) # Original sleep
                            except Exception as queue_err: # Catch error during requeue
                                 self.logger.error(f"{process_name}: Failed to requeue Batch {batch_id}: {queue_err}. Worker stopping.")
                                 raise queue_err # Stop worker if requeue fails
                            # --- Stop processing this batch attempt ---
                            break # Exit the inner while loop

                # Original total_query_counter increment - removed
                # total_query_counter = total_query_counter + 1

            # --- End of inner while loop --- 

            batch_end = time.time()
            batch_elapsed_time = batch_end - batch_start
            # Original batch finished log - adapted slightly for new state info
            # Using original len(query_batch) might be confusing now, using len(all_queries) from state
            self.logger.debug("%s: Query Batch attempt finished: %s ProcessedThisAttempt: %s Requeued: %s TotalInBatch: %s Time: %s",
                              process_name, batch_id, processed_this_attempt, batch_requeued,
                              len(all_queries), # Use length from state dict
                              time.strftime("%H:%M:%S", time.gmtime(batch_elapsed_time)))
            # Original task_done call - kept at the same position relative to outer loop
            Neo4jTransactor.queue.task_done()