from neo4j.v1 import GraphDatabase
import os
import pytest

# Tests for properties under conditional situations.
# e.g. CrossReferences with external URLs when the CrossReference prefix is PANTHER.

def execute_transaction(query):
    host = os.environ['NEO4J_NQC_HOST']
    port = os.environ['NEO4J_NQC_PORT']
    uri = "bolt://" + host + ":" + port
    graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))

    result = None

    with graph.session() as session:
        result = session.run(query)

    return result    

def pytest_generate_tests(metafunc):
    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(argnames, [[funcargs[name] for name in argnames]
            for funcargs in funcarglist])

class TestClass(object):
    # a map specifying multiple argument sets for a test method
    params = {
        'test_prop_with_other_prop': [dict(node1='CrossReference', prop1='MESH', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='NCI', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='ORDO', prop2='crossRefCompleteUrl') ,\
                                      dict(node1='CrossReference', prop1='OMIM', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='EFO', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='KEGG', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='NCIT', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='PANTHER', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='NCBI_Gene', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='UniProtKB', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='ENSEMBL', prop2='crossRefCompleteUrl'), \
                                      dict(node1='CrossReference', prop1='RNACentral', prop2='crossRefCompleteUrl')]
    }

    def test_prop_with_other_prop(self, node1, prop1, prop2):
        query = 'MATCH (n:%s) WHERE n.prefix = \'%s\' AND n.%s is NULL RETURN COUNT(n) as count' % (node1, prop1, prop2)

        result = execute_transaction(query)
        for record in result:
            assert record["count"] == 0