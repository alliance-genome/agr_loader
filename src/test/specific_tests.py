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


def test_fgf8a_exists():
    query = "MATCH (g:Gene) WHERE g.symbol = 'fgf8a' RETURN count(g) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0

# def test_hip1_exists():
#     query = "MATCH (g:Gene) WHERE g.symbol = 'Hip1' RETURN count(g) AS count"
#     result = execute_transaction(query)
#     for record in result:
#         assert record["count"] > 0


def test_doterm_exists():
    query = "MATCH(n:DOTerm) where n.primaryKey = 'DOID:0001816' RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] == 1


def test_isobsolete_false():
    query = "MATCH(n:DOTerm) where n.is_obsolete = 'false' RETURN count(n) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_species_disease_pub_gene_exists():
    query = "MATCH (s:Species)--(g:Gene)--(dg:DiseaseEntityJoin)--(p:Publication) RETURN COUNT(p) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_species_disease_pub_allele_exists():
    query = "MATCH (s:Species)--(f:Feature)--(dg:DiseaseEntityJoin)--(p:Publication) RETURN COUNT(p) AS count"
    result = execute_transaction(query)
    for record in result:
        assert record["count"] > 0


def test_uuid_is_not_duplicated():
    query = "MATCH (g) WITH g.uuid AS uuid, count(*) AS counter WHERE counter > 0 AND g.uuid IS NOT NULL RETURN uuid, counter"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 2


def zfin_gene_has_expression_link():
    query = "MATCH (g:Gene)-[]-(c:CrossReference) where g.primaryKey = 'ZFIN:ZDB-GENE-990415-72' and c.crossRefType = 'gene/expression'"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] > 0


def xref_complete_url_is_formatted():
    query = "MATCH (cr:CrossReference) where not cr.crossRefCompleteUrl =~ 'http%'"
    result = execute_transaction(query)
    for record in result:
        assert record["counter"] < 1