import logging
logger = logging.getLogger(__name__)
from etl import ETL
from .helpers import Neo4jHelper
from transactors import CSVTransactor
import multiprocessing

class ClosureETL(ETL):

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    insert_isa_partof_closure = """
                USING PERIODIC COMMIT %s
                LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                        MATCH (termChild:Ontology {primaryKey:row.child_id})
                        MATCH (termParent:Ontology {primaryKey:row.parent_id})
                        MERGE (termChild)-[closure:IS_A_PART_OF_CLOSURE]->(termParent)
                   """
    retrieve_isa_partof_closure = """
            MATCH (childTerm:Ontology)-[:PART_OF|IS_A*]->(parentTerm:Ontology) 
                RETURN childTerm.primaryKey, parentTerm.primaryKey
                   """

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        for thread in thread_pool:
            thread.join()

    def _process_sub_type(self, subtype):

        logger.info("Starting isa_partof_ Closure")
        query_list = [
            [ClosureETL.insert_isa_partof_closure, "10000",
             "isa_partof_closure_terms.csv"] ,
        ]

        generators = self.get_closure_terms()

        CSVTransactor.save_file_static(generators, query_list)
        logger.info("Finished isa_partof Closure")

    def get_closure_terms(self):

        logger.info("made it to the isa partof closure retrieve")

        returnSet = Neo4jHelper().run_single_query(self.retrieve_isa_partof_closure)

        closure_data = []
        for record in returnSet:
            row = dict(child_id=record["childTerm.primaryKey"],
                       parent_id=record["parentTerm.primaryKey"])
            closure_data.append(row)

        yield [closure_data]
