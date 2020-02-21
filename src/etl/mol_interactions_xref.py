import logging

logger = logging.getLogger(__name__)
import multiprocessing
from .helpers import ETLHelper
from etl import ETL
from transactors import Neo4jTransactor


class MolInteractionsXrefETL(ETL):

    query_xref = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        // This needs to be a MERGE below.
        MATCH (o:InteractionGeneJoin:Association {primaryKey:row.reference_uuid}) """ + ETLHelper.get_cypher_xref_tuned_text()

    xrefs_relationships_template = """
    
        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:InteractionGeneJoin:Association {primaryKey:row.reference_uuid})
            MATCH (c:CrossReference {primaryKey:row.primaryKey})
            
            MERGE (o)-[oc:CROSS_REFERENCE]-(c)
            
    """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        thread_pool = []

        query_tracking_list = multiprocessing.Manager().list()
        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type, query_tracking_list))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

        queries = []
        for item in query_tracking_list:
            queries.append(item)

        Neo4jTransactor.execute_query_batch(queries)

    def _process_sub_type(self):

        all_query_list = [
            [MolInteractionsXrefETL.query_xref, self.data_type_config.commit_size, "mol_int_xref.csv"],
            [MolInteractionsXrefETL.xrefs_relationships_template, self.data_type_config.commit_size, "mol_int_xref.csv"]
        ]

        query_list = self.process_query_params(all_query_list)