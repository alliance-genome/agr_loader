'''Molecular Interactions MOD XREF ETL'''

import logging

from transactors import Neo4jTransactor
from etl import ETL
from .helpers import ETLHelper


class MolInteractionsModXrefETL(ETL):
    '''Motecular Interactoins MOD XREF ETL'''

    logger = logging.getLogger(__name__)

    query_mod_xref_query = """

    USING PERIODIC COMMIT %s
    LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (o:Gene {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_xref_tuned_text()

    xrefs_relationships_query = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Gene {primaryKey:row.dataId})
            MATCH (c:CrossReference:Identifier {primaryKey:row.primaryKey})

            MERGE (o)-[oc:CROSS_REFERENCE]-(c)

    """


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        commit_size = self.data_type_config.get_neo4j_commit_size()

        all_query_list = [
            [self.query_mod_xref_query, commit_size, "mol_int_MOD_xref.csv"],
            [self.xrefs_relationships_query, commit_size, "mol_int_MOD_xref.csv"]
        ]

        query_list = self.process_query_params(all_query_list)
        Neo4jTransactor.execute_query_batch(query_list)
