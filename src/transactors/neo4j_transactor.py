"""Neo4j Transactor"""

import logging
import multiprocessing
import pickle
import queue as stdlib_queue  # For queue.Empty exception
import time
# Import specific exceptions for refined error handling
from neo4j import GraphDatabase, Query, exceptions as neo4j_exceptions
from etl import ETL
from loader_common import ContextInfo


class Neo4jTransactor():
    """Neo4j Transactor"""

    logger = logging.getLogger(__name__)
    count = 0
    queue = None

    # Timeout configuration (in seconds)
    # Connection timeout: Time to establish TCP connection to Neo4j
    CONNECTION_TIMEOUT = 30.0
    # Connection acquisition timeout: Time to wait for a connection from the pool
    CONNECTION_ACQUISITION_TIMEOUT = 120.0
    # Max connection lifetime: Recycle connections after this time to avoid stale connections
    MAX_CONNECTION_LIFETIME = 3600
    # Max connection pool size: Reasonable limit to prevent resource exhaustion
    MAX_CONNECTION_POOL_SIZE = 50
    # Query timeout: Maximum time for a single query (45 minutes for large CSV loads)
    QUERY_TIMEOUT = 2700.0
    # Queue get timeout: Time to wait for next item before logging heartbeat (5 minutes)
    QUEUE_GET_TIMEOUT = 300
    # Max retries for timeout errors (separate from deadlock retries)
    MAX_TIMEOUT_RETRIES = 3

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
        """Execute Query Batch"""

        Neo4jTransactor.count = Neo4jTransactor.count + 1
        batch_id = Neo4jTransactor.count # Added batch_id variable
        queue_size = -1
        try: queue_size = Neo4jTransactor.queue.qsize()
        except NotImplementedError: pass

        # --- Create the stateful batch object --- #
        batch_data = { # New variable/dict holding state
            'all_queries': list(query_batch), # Store original list
            'next_query_index': 0,            # Add starting index
            'retries': 0,                     # Add starting retry counter (for deadlocks)
            'timeout_retries': 0,             # Separate counter for timeout/connection errors
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
        base_retry_sleep = 12 # Base sleep time in seconds for retries

        if context_info.env["USING_PICKLE"] is False:
            # Initialize Neo4j driver with proper timeout configuration
            uri = "bolt://" + context_info.env["NEO4J_HOST"] + ":" + str(context_info.env["NEO4J_PORT"])
            graph = GraphDatabase.driver(
                uri,
                auth=("neo4j", "neo4j"),
                max_connection_pool_size=self.MAX_CONNECTION_POOL_SIZE,
                connection_timeout=self.CONNECTION_TIMEOUT,
                connection_acquisition_timeout=self.CONNECTION_ACQUISITION_TIMEOUT,
                max_connection_lifetime=self.MAX_CONNECTION_LIFETIME,
            )
            self.logger.info("Neo4j driver initialized with timeouts: connection=%ss, acquisition=%ss, lifetime=%ss, pool_size=%s",
                             self.CONNECTION_TIMEOUT, self.CONNECTION_ACQUISITION_TIMEOUT,
                             self.MAX_CONNECTION_LIFETIME, self.MAX_CONNECTION_POOL_SIZE)

        process_name = self._get_name() # Moved after graph init to match original structure closer
        self.logger.info("%s: Starting Neo4jTransactor Thread Runner: ", process_name)
        while True:
            item = None # Added variable for finally check
            batch_data = None # Added variable for state dict
            query_counter = None # Variable for original counter from queue

            # Heartbeat loop: Try to get item with timeout, log heartbeat if waiting
            while True:
                try:
                    self.logger.debug("%s: Heartbeat - waiting for next queue item (timeout=%ss)",
                                      process_name, self.QUEUE_GET_TIMEOUT)
                    # Get item from queue with timeout for heartbeat visibility
                    item = Neo4jTransactor.queue.get(timeout=self.QUEUE_GET_TIMEOUT)
                    (batch_data, query_counter) = item # Unpack item
                    break  # Successfully got item, exit heartbeat loop
                except stdlib_queue.Empty:
                    # No item available - log heartbeat and continue waiting
                    self.logger.info("%s: Heartbeat - worker alive, waiting for work (queue empty for %ss)",
                                     process_name, self.QUEUE_GET_TIMEOUT)
                    continue  # Keep waiting
                except EOFError as error:
                    self.logger.info("Queue Closed exiting: %s", error)
                    if graph:
                        graph.close()
                    return
                except Exception as e:
                    Neo4jTransactor.logger.error(f"{process_name}: Error getting item from queue: {e}. Worker stopping.", exc_info=True)
                    if graph:
                        graph.close()
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
                        # Neo4j execution with query-level timeout
                        if not graph:
                            raise ConnectionError("Neo4j driver not initialized in worker.")
                        # Wrap query with timeout to prevent indefinite hangs on long-running queries
                        query_with_timeout = Query(neo4j_query, timeout=self.QUERY_TIMEOUT)
                        self.logger.debug("%s: Executing query for file %s with timeout=%ss",
                                          process_name, filename, self.QUERY_TIMEOUT)
                        with graph.session() as session:
                            result = session.run(query_with_timeout)
                            result.consume()  # CRITICAL: Ensure query is fully executed (Neo4j uses lazy evaluation)

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
                    batch_data['timeout_retries'] = 0 # Reset timeout retries on success
                    processed_this_attempt += 1 # Increment attempt counter
                    # --- End Success --- # 

                # Specific handling for Neo4j Client Errors (e.g., constraints) - Non-retryable
                except neo4j_exceptions.ClientError as error:
                    self.logger.error(f"{process_name}: Neo4j ClientError processing file {filename} at index {current_index}: {error}", exc_info=True)
                    # Check if it's a constraint violation
                    if hasattr(error, 'code') and 'ConstraintValidationFailed' in error.code:
                        self.logger.critical(
                            "%s: Constraint violation, aborting processing for file: %s. Worker stopping.",
                            process_name, filename)
                    else:
                         self.logger.error(
                            "%s: Unrecoverable ClientError processing file: %s. Worker stopping.",
                            process_name, filename)
                    raise error # Stop worker for all ClientErrors

                # Specific handling for Neo4j Transient Errors (e.g., deadlocks) - Retryable
                except neo4j_exceptions.TransientError as error:
                    # Log transient errors without full traceback unless debugging needed
                    self.logger.error(f"{process_name}: Neo4j TransientError processing file {filename} at index {current_index}: {error}", exc_info=False)
                    batch_data['retries'] += 1
                    self.logger.warning(
                        "%s: Query Conflict (TransientError), putting data back in Queue to run later. File: %s (Retry %d/%d)",
                        process_name, filename, batch_data['retries'], max_retries)

                    if batch_data['retries'] > max_retries:
                        # FAIL LOUDLY - 100% of data must be loaded
                        self.logger.critical(
                            "%s: FATAL - Max retries (%d) exceeded for file: %s at Index %s. "
                            "Data cannot be loaded. Failing program.",
                            process_name, max_retries, filename, current_index)
                        raise RuntimeError(
                            f"Max retries ({max_retries}) exceeded for file {filename} at index {current_index}. "
                            f"TransientError (deadlock) could not be resolved. Data load failed."
                        )
                    # Else perform requeue and incremental backoff
                    try:
                        Neo4jTransactor.queue.put((batch_data, query_counter))
                        batch_requeued = True
                        # Incremental backoff for sleep time
                        sleep_time = base_retry_sleep + batch_data['retries']
                        self.logger.info(f"{process_name}: Sleeping for {sleep_time} seconds before retry for batch {batch_id}")
                        time.sleep(sleep_time)
                    except Exception as queue_err:
                        self.logger.error(f"{process_name}: Failed to requeue Batch {batch_id} after TransientError: {queue_err}. Worker stopping.", exc_info=True)
                        raise queue_err
                    break  # exit inner loop to wait before next attempt

                # Handle ServiceUnavailable (connection lost, timeout, etc.) - Retryable
                except neo4j_exceptions.ServiceUnavailable as error:
                    self.logger.error(f"{process_name}: Neo4j ServiceUnavailable for file {filename} at index {current_index}: {error}", exc_info=False)
                    batch_data['timeout_retries'] += 1
                    self.logger.warning(
                        "%s: Connection/timeout error, will retry. File: %s (Timeout Retry %d/%d)",
                        process_name, filename, batch_data['timeout_retries'], self.MAX_TIMEOUT_RETRIES)

                    if batch_data['timeout_retries'] > self.MAX_TIMEOUT_RETRIES:
                        # FAIL LOUDLY - 100% of data must be loaded
                        self.logger.critical(
                            "%s: FATAL - Max timeout retries (%d) exceeded for file: %s at Index %s. "
                            "Neo4j connection unavailable. Failing program.",
                            process_name, self.MAX_TIMEOUT_RETRIES, filename, current_index)
                        raise RuntimeError(
                            f"Max timeout retries ({self.MAX_TIMEOUT_RETRIES}) exceeded for file {filename} at index {current_index}. "
                            f"ServiceUnavailable - Neo4j connection could not be established. Data load failed."
                        )
                    # Requeue with longer backoff for connection issues
                    try:
                        # Recreate driver in case connection is completely broken
                        if graph:
                            try:
                                graph.close()
                            except Exception:
                                pass
                        uri = "bolt://" + context_info.env["NEO4J_HOST"] + ":" + str(context_info.env["NEO4J_PORT"])
                        graph = GraphDatabase.driver(
                            uri,
                            auth=("neo4j", "neo4j"),
                            max_connection_pool_size=self.MAX_CONNECTION_POOL_SIZE,
                            connection_timeout=self.CONNECTION_TIMEOUT,
                            connection_acquisition_timeout=self.CONNECTION_ACQUISITION_TIMEOUT,
                            max_connection_lifetime=self.MAX_CONNECTION_LIFETIME,
                        )
                        graph.verify_connectivity()  # Fail fast if Neo4j is truly down
                        self.logger.info(f"{process_name}: Recreated Neo4j driver after connection error")

                        Neo4jTransactor.queue.put((batch_data, query_counter))
                        batch_requeued = True
                        # Longer backoff for connection issues (30 seconds base + retries * 10)
                        sleep_time = 30 + (batch_data['timeout_retries'] * 10)
                        self.logger.info(f"{process_name}: Sleeping for {sleep_time} seconds before retry after connection error")
                        time.sleep(sleep_time)
                    except Exception as queue_err:
                        self.logger.critical(f"{process_name}: CRITICAL - Failed to requeue after ServiceUnavailable: {queue_err}. DATA MAY BE LOST!", exc_info=True)
                        raise queue_err
                    break

                # Handle SessionExpired (session timeout) - Retryable
                except neo4j_exceptions.SessionExpired as error:
                    self.logger.error(f"{process_name}: Neo4j SessionExpired for file {filename} at index {current_index}: {error}", exc_info=False)
                    batch_data['timeout_retries'] += 1
                    self.logger.warning(
                        "%s: Session expired, will retry. File: %s (Timeout Retry %d/%d)",
                        process_name, filename, batch_data['timeout_retries'], self.MAX_TIMEOUT_RETRIES)

                    if batch_data['timeout_retries'] > self.MAX_TIMEOUT_RETRIES:
                        # FAIL LOUDLY - 100% of data must be loaded
                        self.logger.critical(
                            "%s: FATAL - Max timeout retries (%d) exceeded for file: %s at Index %s. "
                            "Session expired repeatedly. Failing program.",
                            process_name, self.MAX_TIMEOUT_RETRIES, filename, current_index)
                        raise RuntimeError(
                            f"Max timeout retries ({self.MAX_TIMEOUT_RETRIES}) exceeded for file {filename} at index {current_index}. "
                            f"SessionExpired - Neo4j session could not be maintained. Data load failed."
                        )
                    # Requeue with backoff
                    try:
                        Neo4jTransactor.queue.put((batch_data, query_counter))
                        batch_requeued = True
                        sleep_time = 20 + (batch_data['timeout_retries'] * 5)
                        self.logger.info(f"{process_name}: Sleeping for {sleep_time} seconds before retry after session expiry")
                        time.sleep(sleep_time)
                    except Exception as queue_err:
                        self.logger.critical(f"{process_name}: CRITICAL - Failed to requeue after SessionExpired: {queue_err}. DATA MAY BE LOST!", exc_info=True)
                        raise queue_err
                    break

                # Handle potential connection issues explicitly (driver not initialized)
                except ConnectionError as error:
                    self.logger.critical(
                        "%s: FATAL - ConnectionError for file %s at index %s: %s. "
                        "Neo4j driver not available. Failing program.",
                        process_name, filename, current_index, error)
                    raise RuntimeError(
                        f"ConnectionError for file {filename} at index {current_index}: {error}. "
                        f"Neo4j driver not available. Data load failed."
                    ) from error

                # Catch-all for other unexpected errors
                except Exception as error:
                    self.logger.critical(
                        "%s: FATAL - Unexpected error for file %s at index %s: %s. Failing program.",
                        process_name, filename, current_index, error, exc_info=True)
                    raise RuntimeError(
                        f"Unexpected error for file {filename} at index {current_index}: {error}. "
                        f"Data load failed."
                    ) from error

            # --- End of inner while loop --- 

            batch_end = time.time()
            batch_elapsed_time = batch_end - batch_start
            # Original batch finished log - adapted slightly for new state info
            # Using original len(query_batch) might be confusing now, using len(all_queries) from state
            self.logger.debug("%s: Query Batch attempt finished: %s ProcessedThisAttempt: %s Requeued: %s TotalInBatch: %s Time: %s",
                              process_name, batch_id, processed_this_attempt, batch_requeued,
                              len(all_queries), # Use length from state dict
                              time.strftime("%H:%M:%S", time.gmtime(batch_elapsed_time)))
            # ALWAYS call task_done() - each get() from queue needs a corresponding task_done()
            # Requeued items are NEW tasks that will get their own task_done() when processed
            Neo4jTransactor.queue.task_done()