"""Molecular Interactions XREF ETL."""

import logging

from etl import ETL
from transactors import Neo4jTransactor
from .helpers import ETLHelper


class MolInteractionsXrefETL(ETL):
    """Molecular Interactions XREF ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    query_xref_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                // This needs to be a MERGE below.
                MATCH (o:InteractionGeneJoin:Association {primaryKey:row.reference_uuid})
                """ + ETLHelper.get_cypher_xref_tuned_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    xrefs_relationships_query_template = """

        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (o:InteractionGeneJoin:Association {primaryKey:row.reference_uuid})
                MATCH (c:CrossReference {primaryKey:row.primaryKey})

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
            [self.query_xref_query_template, "mol_int_xref.csv", commit_size],
            [self.xrefs_relationships_query_template, "mol_int_xref.csv", commit_size]
        ]

        query_list = self.process_query_params(query_template_list)
        Neo4jTransactor.execute_query_batch(query_list)
