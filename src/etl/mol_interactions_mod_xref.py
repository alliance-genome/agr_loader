import logging
logger = logging.getLogger(__name__)

from .helpers import ResourceDescriptorHelper2, ETLHelper
from etl import ETL
from transactors import Neo4jTransactor


class MolInteractionsModXrefETL(ETL):

    query_mod_xref = """

    USING PERIODIC COMMIT %s
    LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (o:Gene {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_xref_text()

    xrefs_relationships_template = """

        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Gene {primaryKey:row.dataId})
            MATCH (c:CrossReference {globalCrossRefId:row.globalCrossRefId})

            MERGE (o)-[oc:CROSS_REFERENCE]-(c)

    """


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        commit_size = self.data_type_config.get_neo4j_commit_size()

        all_query_list = [
                     [MolInteractionsModXrefETL.query_mod_xref, commit_size, "mol_int_MOD_xref.csv"],
                     [MolInteractionsModXrefETL.xrefs_relationships_template, commit_size, "mol_int_MOD_xref.csv"]
                     ]

        query_list = self.process_query_params(all_query_list)
        Neo4jTransactor.execute_query_batch(query_list)