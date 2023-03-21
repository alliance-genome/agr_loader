"""Molecular Interactions MOD XREF ETL."""

import logging

from transactors import Neo4jTransactor
from etl import ETL
from .helpers import ETLHelper


class MolInteractionsModXrefETL(ETL):
    """Motecular Interactoins MOD XREF ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    query_mod_xref_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (o:Gene {primaryKey:row.dataId}) 
                """ + ETLHelper.get_cypher_xref_tuned_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    xrefs_relationships_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (o:Gene {primaryKey:row.dataId})
                MATCH (c:CrossReference:Identifier {primaryKey:row.primaryKey})

                MERGE (o)-[oc:CROSS_REFERENCE]-(c)
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        commit_size = self.data_type_config.get_neo4j_commit_size()

        query_template_list = [
            [self.query_mod_xref_query_template, "mol_int_MOD_xref.csv", commit_size],
            [self.xrefs_relationships_query_template, "mol_int_MOD_xref.csv", commit_size]
        ]

        query_list = self.process_query_params(query_template_list)
        Neo4jTransactor.execute_query_batch(query_list)
