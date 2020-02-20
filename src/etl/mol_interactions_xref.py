import logging

logger = logging.getLogger(__name__)

from .helpers import ResourceDescriptorHelper2, ETLHelper
from etl import ETL
from transactors import Neo4jTransactor


class MolInteractionsXrefETL(ETL):

    query_xref = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        // This needs to be a MERGE below.
        MATCH (o:InteractionGeneJoin:Association) WHERE o.primaryKey = row.reference_uuid """ + ETLHelper.get_cypher_xref_text()

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

        # Initialize an instance of ResourceDescriptor for processing external links.
        self.resource_descriptor_dict = ResourceDescriptorHelper2()
        self.missed_database_linkouts = set()
        self.successful_database_linkouts = set()
        self.ignored_database_linkouts = set()
        self.successful_MOD_interaction_xrefs = []

    def _load_and_process_data(self):
        commit_size = self.data_type_config.get_neo4j_commit_size()

        all_query_list = [
            [MolInteractionsXrefETL.query_xref, commit_size, "mol_int_xref.csv"]
        ]

        query_list = self.process_query_params(all_query_list)
        Neo4jTransactor.execute_query_batch(query_list)