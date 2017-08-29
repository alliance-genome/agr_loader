from neo4j.v1 import GraphDatabase
import os

def execute_transaction(query):
    host = os.environ['NEO4J_NQC_HOST']
    port = os.environ['NEO4J_NQC_PORT']
    uri = "bolt://" + host + ":" + port
    graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))

    result = None

    with graph.session() as session:
        result = session.run(query)

    return result

def test_gene():
    query = "MATCH (g:Gene) WHERE g.primaryKey = 'MGI:2676586' RETURN g.primaryKey AS primaryKey"
    result = execute_transaction(query)
    record = result.single()
    assert record["primaryKey"] == 'MGI:2676586'