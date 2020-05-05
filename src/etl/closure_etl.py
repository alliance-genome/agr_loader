import logging
logger = logging.getLogger(__name__)
from etl import ETL
from .helpers import Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor
import multiprocessing

class ClosureETL(ETL):


    insert_isa_partof_closure = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (termChild:Ontology:%sTerm {primaryKey:row.child_id})
            MATCH (termParent:Ontology:%sTerm {primaryKey:row.parent_id})
            CREATE (termChild)-[closure:IS_A_PART_OF_CLOSURE]->(termParent) """
    
    retrieve_isapartof_closure = """
        MATCH (childTerm:Ontology:%sTerm)-[r:PART_OF|IS_A*]->(parentTerm:%sTerm) 
            RETURN distinct childTerm.primaryKey, parentTerm.primaryKey """


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):
        data_provider = sub_type.get_data_provider()
        logger.info(data_provider)
        if data_provider == 'DOID':
            data_provider = 'DO'
        
        logger.debug("Starting isa_partof_ Closure for: %s" % data_provider)
        
        query_list = [
            [ClosureETL.insert_isa_partof_closure, "10000", "isa_closure_" + data_provider + ".csv",
             data_provider, data_provider]
        ]

        generators = self.get_closure_terms(data_provider)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        
        logger.debug("Finished isa_partof Closure for: %s" % data_provider)

    def get_closure_terms(self, data_provider):

        query_isapartof = self.retrieve_isapartof_closure % (data_provider, data_provider)

        logger.debug("Query to Run: %s" % query_isapartof)

        returnSet = Neo4jHelper().run_single_query(query_isapartof)

        closure_data_isapartof = []
        for record in returnSet:
            row = dict(child_id=record["childTerm.primaryKey"],
                       parent_id=record["parentTerm.primaryKey"])
            closure_data_isapartof.append(row)


        yield [closure_data_isapartof]
