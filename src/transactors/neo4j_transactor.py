import logging
import multiprocessing
import pickle
import time
# Import specific exceptions for better handling
from neo4j import GraphDatabase, exceptions as neo4j_exceptions
from etl import ETL # Assuming ETL.wait_for_threads exists
from loader_common import ContextInfo # Assuming ContextInfo is defined

# Sentinel object to signal shutdown
SHUTDOWN_SENTINEL = None

class Neo4jTransactor():
    """
    Neo4j Transactor using multiprocessing.
    Tracks progress within batches. Skips ConstraintViolation errors.
    Retries TransientErrors. Includes Graceful Shutdown.
    Refined error logging to reduce noise for handled exceptions.
    """

    logger = logging.getLogger(__name__)
    # Class variables managed by the main process or using Manager
    count = 0
    queue = None

    def __init__(self):
        self.process_pool = [] # Internally still uses processes
        self._manager = None
        self._process_count = 0 # Store process count

    @staticmethod
    def _get_name():
        # Get a descriptive name for the current process
        return f"Neo4jTransactor-{multiprocessing.current_process().name}"

    # Method name reverted back to start_threads
    def start_threads(self, process_count): # Renamed back from start_processes
        """Start the worker processes (named start_threads for compatibility)."""
        if self.process_pool:
            self.logger.warning("Processes already started.")
            return

        # Log the requested count
        self.logger.info(f"Neo4jTransactor start_threads called with process_count = {process_count}")

        self._process_count = process_count
        # Use Manager's Queue for safe inter-process sharing
        if not self._manager:
             self._manager = multiprocessing.Manager()
        Neo4jTransactor.queue = self._manager.Queue()

        self.logger.info(f"Starting {self._process_count} Neo4jTransactor worker processes...")
        for i in range(self._process_count):
            # Pass the run method of the instance
            process = multiprocessing.Process(target=self.run, name=str(i), daemon=True)
            process.start()
            self.process_pool.append(process)
        self.logger.info("Worker processes started.")


    # Shutdown method updated for Graceful Shutdown
    def shutdown(self, wait_timeout=30): # Added timeout parameter
        """Initiates graceful shutdown of worker processes."""
        if not self.process_pool or Neo4jTransactor.queue is None:
            self.logger.info("No active processes or queue to shut down.")
            return

        process_count_to_signal = self._process_count
        self.logger.info(
            f"Sending shutdown sentinel ({SHUTDOWN_SENTINEL}) "
            f"to {process_count_to_signal} worker processes via queue..."
        )
        # Send one sentinel for each worker process expected
        for _ in range(process_count_to_signal):
            try:
                Neo4jTransactor.queue.put(SHUTDOWN_SENTINEL, timeout=5)
            except multiprocessing.queues.Full:
                 self.logger.error("Queue is full while trying to send shutdown sentinel.")
                 break
            except Exception as e:
                self.logger.error(f"Error putting sentinel onto queue: {e}")

        self.logger.info(f"Waiting up to {wait_timeout} seconds for processes to finish gracefully...")
        start_time = time.time()
        processes_to_check = list(self.process_pool) # Copy list
        joined_processes = set()

        while (time.time() - start_time) < wait_timeout:
            all_joined = True
            for process in processes_to_check:
                 if process in joined_processes: continue
                 process.join(timeout=0.1)
                 if process.is_alive():
                     all_joined = False
                 else:
                     self.logger.debug(f"Process {process.name} joined gracefully.")
                     joined_processes.add(process)
            if all_joined: break
            time.sleep(0.5)

        # Check which processes are still alive after the wait period
        alive_processes = [p for p in self.process_pool if p.is_alive()]
        if alive_processes:
            self.logger.warning(
                f"{len(alive_processes)} processes did not exit gracefully after {wait_timeout}s. Terminating them..."
            )
            for process in alive_processes:
                try:
                    self.logger.warning(f"Terminating process {process.name}...")
                    process.terminate()
                    process.join(timeout=1)
                    if process.is_alive(): self.logger.error(f"Process {process.name} could not be terminated.")
                except Exception as e: self.logger.error(f"Error during forced termination of process {process.name}: {e}")
        else:
            self.logger.info("All processes shut down gracefully.")

        self.logger.info("Finished shutting down Neo4jTransactor processes.")
        # Clean up resources
        self.process_pool = []
        self._process_count = 0
        Neo4jTransactor.queue = None # Clear queue reference


    @staticmethod
    def execute_query_batch(query_batch):
        """
        Submits a batch of queries, wrapping it in a stateful dictionary
        for progress tracking.
        """
        if Neo4jTransactor.queue is None:
            Neo4jTransactor.logger.error("Queue not initialized. Call start_threads first.")
            return

        Neo4jTransactor.count += 1
        batch_id = Neo4jTransactor.count
        queue_size = -1
        try:
             queue_size = Neo4jTransactor.queue.qsize()
        except NotImplementedError: pass

        # Create the stateful batch object
        batch_data = {
            'all_queries': list(query_batch),
            'next_query_index': 0,
            'retries': 0,
            'batch_id': batch_id
        }

        Neo4jTransactor.logger.debug(f"Adding Batch {batch_id} Size: {len(query_batch)} ApproxQueueSize: {queue_size}")
        Neo4jTransactor.queue.put((batch_data, batch_id))

    # Method name reverted back to check_for_thread_errors
    def check_for_thread_errors(self): # Renamed back from check_for_process_errors
        """
        Checks if any worker processes have exited unexpectedly (named check_for_thread_errors for compatibility).
        Relies on an external function ETL.wait_for_threads.
        """
        if Neo4jTransactor.queue is None or not self.process_pool:
            self.logger.debug("Skipping check_for_thread_errors: Queue or process pool not initialized.")
            return
        try:
            self.logger.debug(f"Checking for errors in {len(self.process_pool)} processes.")
            ETL.wait_for_threads(self.process_pool, Neo4jTransactor.queue)
        except Exception as e:
            self.logger.error(f"Error during check_for_thread_errors: {e}", exc_info=True)


    # wait_for_queues method corrected to use Class.logger
    @staticmethod
    def wait_for_queues():
        """Waits until all items submitted to the queue have been processed."""
        logger = Neo4jTransactor.logger # Get logger via class name
        if Neo4jTransactor.queue:
            qsize = -1
            try:
                qsize = Neo4jTransactor.queue.qsize()
            except NotImplementedError: pass
            logger.info(f"Waiting for queue to empty (approx size: {qsize})...")
            Neo4jTransactor.queue.join()
            logger.info("Queue processing finished.")
        else:
            logger.warning("Queue not initialized, cannot wait.")

    # ====================================================================
    # Run method - Refined Error Handling and Logging
    # ====================================================================
    def run(self):
        """
        Main execution loop for worker processes. Refined error logging.
        """
        process_name = self._get_name()
        context_info = ContextInfo()
        graph = None
        max_retries = context_info.env.get('NEO4J_MAX_RETRIES', 10)
        retry_delay = context_info.env.get('NEO4J_RETRY_DELAY', 12)

        # Outer try block encompasses connection and the main processing loop
        try:
            # Establish Neo4j Connection
            if not context_info.env.get("USING_PICKLE", False):
                try:
                    # Inner try specifically for connection setup
                    uri = f"bolt://{context_info.env['NEO4J_HOST']}:{context_info.env['NEO4J_PORT']}"
                    auth = (context_info.env.get('NEO4J_USER', 'neo4j'), context_info.env.get('NEO4J_PASSWORD', 'neo4j'))
                    pool_size = context_info.env.get('NEO4J_POOL_SIZE', -1)
                    fetch_size = context_info.env.get('NEO4J_FETCH_SIZE', 1000)
                    graph = GraphDatabase.driver(uri, auth=auth, max_connection_pool_size=pool_size, fetch_size=fetch_size)
                    graph.verify_connectivity()
                    self.logger.info(f"{process_name}: Neo4j Driver created for {uri}")
                except Exception as e:
                    self.logger.critical(f"{process_name}: Failed to create Neo4j driver: {e}. Worker stopping.", exc_info=True)
                    return # Stop this worker process

            self.logger.info(f"{process_name}: Starting Neo4jTransactor Worker")

            # Main processing loop is now inside the outer try block
            while True:
                item = None
                batch_data = None
                query_counter = None

                # Inner try...finally for handling each queue item
                try:
                    # Blocking get from the queue
                    item = Neo4jTransactor.queue.get()

                    # Check for shutdown sentinel
                    if item is SHUTDOWN_SENTINEL:
                        self.logger.info(f"{process_name}: Received shutdown sentinel. Exiting loop.")
                        break # Exit the main while loop cleanly

                    # Unpack the received item
                    (batch_data, query_counter) = item

                    # --- Batch Processing Logic ---
                    batch_id = batch_data['batch_id']
                    start_index = batch_data['next_query_index']
                    total_queries_in_batch = len(batch_data['all_queries'])

                    self.logger.debug(
                        f"{process_name}: Processing Batch {batch_id} "
                        f"from index {start_index}/{total_queries_in_batch} "
                        f"(Retry count for current index: {batch_data['retries']})"
                    )
                    batch_start_time = time.time()
                    processed_count_this_attempt = 0
                    batch_requeued_this_attempt = False

                    # --- Inner loop iterates using the index ---
                    while batch_data['next_query_index'] < total_queries_in_batch:
                        current_index = batch_data['next_query_index']
                        (neo4j_query, filename) = batch_data['all_queries'][current_index]

                        self.logger.debug(f"{process_name}: Batch {batch_id}, Attempting Index {current_index}, File: {filename}")
                        query_start_time = time.time()

                        try:
                            # Execute the query (Pickle or Neo4j)
                            if context_info.env.get("USING_PICKLE", False):
                                pickle_dir = context_info.env.get("PICKLE_PATH", "tmp/temp")
                                file_name = f"{pickle_dir}/transaction_{batch_id}_{current_index}.pkl"
                                with open(file_name, 'wb') as file: pickle.dump(neo4j_query, file)
                                self.logger.debug(f"{process_name}: Pickled Batch {batch_id}, Index {current_index} to {file_name}")
                            else:
                                if not graph: raise ConnectionError("Neo4j driver not initialized.")
                                with graph.session() as session: session.run(neo4j_query)

                            # Query Succeeded
                            query_end_time = time.time(); elapsed_time = query_end_time - query_start_time
                            self.logger.info(f"{process_name}: OK Batch {batch_id}, Index {current_index}, File: {filename}, Time: {elapsed_time:.2f}s")
                            batch_data['next_query_index'] += 1
                            batch_data['retries'] = 0 # Reset retries on success
                            processed_count_this_attempt += 1

                        # ===========================================================
                        # Refined Exception Handling for Logging Control
                        # ===========================================================
                        except neo4j_exceptions.ConstraintError as error:
                            # Constraint Error: Log CRITICAL, Skip query, NO traceback
                            self.logger.critical(
                                f"{process_name}: Constraint violation Batch {batch_id}, Index {current_index}, "
                                f"File: {filename}: {error}. Skipping query."
                            )
                            # Log the query that failed for debugging (optional)
                            # self.logger.error(f"{process_name}: Failed Query causing constraint error: {neo4j_query}")
                            batch_data['next_query_index'] += 1 # Skip failed query
                            batch_data['retries'] = 0 # Reset retries
                            continue # Go to next iteration

                        except (neo4j_exceptions.TransientError,
                                neo4j_exceptions.ServiceUnavailable,
                                neo4j_exceptions.DatabaseError) as error:
                            # Known Transient/Retryable Errors: Log WARNING, Requeue state, NO traceback
                            batch_data['retries'] += 1
                            self.logger.warning(
                                f"{process_name}: Transient error on Batch {batch_id}, Index {current_index}, "
                                f"File: {filename}: {error} " # Log error message itself
                                f"(Retry {batch_data['retries']}/{max_retries}). Requeuing batch state."
                            )
                            # Log the query that failed for debugging (optional)
                            # self.logger.error(f"{process_name}: Failed Query causing transient error: {neo4j_query}")

                            if batch_data['retries'] > max_retries:
                                self.logger.error(
                                    f"{process_name}: Max retries exceeded for Batch {batch_id} "
                                    f"at Index {current_index}. Aborting batch processing."
                                )
                                batch_requeued_this_attempt = False
                                break # Exit inner loop
                            else: # Requeue
                                try:
                                    Neo4jTransactor.queue.put((batch_data, query_counter))
                                    batch_requeued_this_attempt = True
                                    self.logger.info(f"{process_name}: Requeued Batch {batch_id} state for retry from index {current_index}.")
                                    time.sleep(retry_delay)
                                except Exception as queue_err:
                                     self.logger.error(f"{process_name}: Failed to requeue Batch {batch_id}: {queue_err}")
                                     batch_requeued_this_attempt = False
                                break # Exit inner loop (stop this attempt)

                        except Exception as error:
                            # Unexpected Errors: Log ERROR WITH traceback, Skip query (for safety)
                            self.logger.error(
                                f"{process_name}: UNEXPECTED error processing Batch {batch_id}, Index {current_index}, "
                                f"File: {filename}: {error}. Skipping query.",
                                exc_info=True # Include traceback for unexpected errors
                            )
                            # Log the query that failed for debugging (optional)
                            # self.logger.error(f"{process_name}: Failed Query causing unexpected error: {neo4j_query}")
                            batch_data['next_query_index'] += 1 # Skip failed query
                            batch_data['retries'] = 0 # Reset retries
                            continue # Go to next iteration (treat unexpected errors as non-retryable for this query)
                        # ===========================================================
                        # End Refined Exception Handling
                        # ===========================================================

                    # --- End of inner while loop ---

                    # Log outcome of this processing attempt
                    batch_end_time = time.time(); batch_elapsed = batch_end_time - batch_start_time
                    if not batch_requeued_this_attempt:
                        final_index = batch_data['next_query_index']
                        if final_index == total_queries_in_batch:
                            self.logger.info(f"{process_name}: Batch {batch_id} completed successfully. Processed this attempt: {processed_count_this_attempt}. Total Time: {batch_elapsed:.2f}s")
                        else:
                            self.logger.error(f"{process_name}: Batch {batch_id} finished incompletely. Stopped at index {final_index}/{total_queries_in_batch}. Processed this attempt: {processed_count_this_attempt}. Total Time: {batch_elapsed:.2f}s")

                # --- Handle Worker Loop Get/Setup Exceptions ---
                except EOFError:
                    self.logger.warning(f"{process_name}: Queue closed unexpectedly. Exiting.")
                    break # Exit outer while loop
                except BrokenPipeError:
                     self.logger.warning(f"{process_name}: Queue connection broken. Exiting.")
                     break # Exit outer while loop
                except Exception as e:
                    # Catch errors in getting from queue or general processing setup
                    self.logger.error(f"{process_name}: Unhandled error in worker main loop section: {e}", exc_info=True)
                    time.sleep(5) # Avoid tight loop
                finally:
                    # --- IMPORTANT: Mark task as done ---
                    if item is not None and item is not SHUTDOWN_SENTINEL:
                        try:
                             Neo4jTransactor.queue.task_done()
                        except ValueError:
                             self.logger.warning(f"{process_name}: ValueError calling task_done(). Might be expected during shutdown.")
                        except Exception as e:
                             self.logger.error(f"{process_name}: Error calling task_done(): {e}")

        # --- Worker Exit Cleanup ---
        finally:
             self.logger.info(f"{process_name}: Worker loop finished. Cleaning up resources...")
             if graph:
                 try:
                     graph.close()
                     self.logger.info(f"{process_name}: Neo4j Driver closed.")
                 except Exception as e:
                     self.logger.error(f"{process_name}: Error closing Neo4j Driver: {e}")
             self.logger.info(f"{process_name}: Worker exiting.")