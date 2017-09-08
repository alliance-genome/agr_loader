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

def test_do():
    query = "MATCH (n:DOTerm) WHERE n.name IS NULL RETURN count(n) AS count"
    result = execute_transaction(query)
    record = result.single()
    assert record["count"] == 1

def test_for_dupe_genes():
    query = "MATCH (g:Gene) WHERE keys(g)[0] = 'primaryKey' and size(keys(g)) = 1 RETURN g.primaryKey, keys(g)"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 0

def test_fgf8a_exists():
    query = "MATCH (g:Gene) WHERE g.symbol = 'fgf8a'"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

def test_hip1_exists():
    query = "MATCH (g:Gene) WHERE g.symbol = 'Hip1'"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

def test_doterm_exists():
    query = "MATCH(n:DOTerm) where n.primaryKey = 'DOID:0060348'"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 1
