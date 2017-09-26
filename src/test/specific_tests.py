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

def test_for_dupe_genes():
    query = "MATCH (g:Gene) WHERE keys(g)[0] = 'primaryKey' and size(keys(g)) = 1 RETURN count(g) as count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 0

def test_fgf8a_exists():
    query = "MATCH (g:Gene) WHERE g.symbol = 'fgf8a' RETURN count(g) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

def test_hip1_exists():
    query = "MATCH (g:Gene) WHERE g.symbol = 'Hip1' RETURN count(g) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

def test_doterm_exists():
    query = "MATCH(n:DOTerm) where n.primaryKey = 'DOID:0001816' RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 1

def test_pubMedUrl_exists():
    query = "MATCH(n:Publication) where n.pubMedUrl IS NOT NULL RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

def test_isobsolete_false():
    query = "MATCH(n:DOTerm) where n.is_obsolete = 'false' RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

def test_gene_xref_exists():
    query = "MATCH p=(g:Gene)--(c:CrossReference) WHERE g.primaryKey = 'RGD:61995' RETURN COUNT(p) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

def test_gene_xref_url_exists():
    query = "MATCH p=(g:Gene)--(c:CrossReference) WHERE g.primaryKey = 'RGD:61995' AND c.crossRefCompleteUrl is not null RETURN COUNT(p) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

def test_species_disease_pub_exists():
    query = "MATCH (s:Species)--(g:Gene)--(dg:DiseaseGeneJoin)--(p:Publication) RETURN COUNT(p) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0




#
# def test_defLinks():
#     query = "MATCH(n:DOTerm) WITH length(n.defLinksProcessed) as linkCount where n.primaryKey = 'DOID:1335' RETURN linkCount AS count"
#     result = execute_transaction(query)
#     for record in result:
#         assert record["count"] > 1


