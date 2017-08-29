from neo4j.v1 import GraphDatabase
import pytest
import os

class UnitTest(object):

    def __init__(self):
        # Run all unit tests.
        self.test_gene()

    def execute_transaction(self, query):
        host = os.environ['NEO4J_NQC_HOST']
        port = os.environ['NEO4J_NQC_PORT']
        uri = "bolt://" + host + ":" + port
        graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))

        result = None

        with graph.session() as session:
            result = session.run(query)

        return result

    def test_gene(self):
        query = "MATCH (g:Gene) WHERE g.primaryKey = 'MGI:107956' RETURN g.primaryKey AS primaryKey"
        result = self.execute_transaction(query)
        for record in result:
            assert record["primaryKey"] == 'MGI:107956'