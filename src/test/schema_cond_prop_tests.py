"""Schema String Property Test"""

from etl import Neo4jHelper


def pytest_generate_tests(metafunc):
    """pyTest Generat Test"""

    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(argnames, [[funcargs[name] for name in argnames] \
                                     for funcargs in funcarglist])


class TestClass(object):
    """A map specifying multiple argument sets for a test method"""
    params = {
        'test_prop_with_other_prop': [dict(node1='CrossReference',
                                           prop1='MESH',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='NCI',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='ORDO',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='OMIM',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='EFO',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='KEGG',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='NCIT',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='PANTHER',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='NCBI_Gene',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='UniProtKB',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='ENSEMBL',
                                           prop2='crossRefCompleteUrl'),
                                      dict(node1='CrossReference',
                                           prop1='RNACentral',
                                           prop2='crossRefCompleteUrl')]
    }

    @staticmethod
    def test_prop_with_other_prop(node1, prop1, prop2):
        """Test Property with Other Property"""

        query = """MATCH (n:%s)
                   WHERE n.prefix = \'%s\'
                         AND n.%s is NULL
                   RETURN COUNT(n) as count""" % (node1, prop1, prop2)

        with Neo4jHelper.run_single_query(query) as result:
            for record in result:
                assert record["count"] == 0
