from neo4j.v1 import GraphDatabase
import os
import pytest


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
        'test_node_exists': [dict(node='Ontology'), \
                             dict(node='SOTerm'), \
                             dict(node='DOTerm'), \
                             dict(node='GOTerm'), \
                             dict(node='MITerm'), \
                             dict(node='Identifier'), \
                             dict(node='Gene'), \
                             dict(node='Synonym'), \
                             dict(node='CrossReference'), \
                             dict(node='Species'), \
                             dict(node='Entity'), \
                             dict(node='Chromosome'), \
                             dict(node='DiseaseEntityJoin'), \
                             dict(node='Association'), \
                             dict(node='Publication'), \
                             dict(node='EvidenceCode'), \
                             dict(node='Feature'), \
                             dict(node='Phenotype'), \
                             dict(node='PhenotypeEntityJoin'), \
                             dict(node='OrthologyGeneJoin'), \
                             dict(node='OrthoAlgorithm'), \
                             dict(node='Load'), \
                             dict(node='Feature'), \
                             dict(node='ExpressionBioEntity'), \
                             dict(node='Stage'), \
                             dict(node='BioEntityGeneExpressionJoin'), \
                             dict(node='InteractionGeneJoin'), \
                             ],

        'test_prop_exist': [dict(node='Gene', prop='modGlobalCrossRefId'), \
                            dict(node='Gene', prop='dateProduced'), \
                            dict(node='Gene', prop='geneLiteratureUrl'), \
                            dict(node='Gene', prop='modCrossRefCompleteUrl'), \
                            dict(node='Gene', prop='taxonId'), \
                            dict(node='Gene', prop='geneticEntityExternalUrl'), \
                            dict(node='Gene', prop='modLocalId'), \
                            dict(node='Gene', prop='symbol'), \
                            dict(node='Gene', prop='primaryKey'), \
                            dict(node='Gene', prop='modGlobalId'), \
                            dict(node='Gene', prop='uuid'), \
                            dict(node='GOTerm', prop='primaryKey'), \
                            dict(node='Gene', prop='dataProvider'), \
                            #dict(node='SOTerm', prop='name'), \
                            dict(node='SOTerm', prop='primaryKey'), \
                            dict(node='DOTerm', prop='doPrefix'), \
                            dict(node='DOTerm', prop='doId'), \
                            dict(node='DOTerm', prop='doDisplayId'), \
                            dict(node='DOTerm', prop='doUrl'), \
                            dict(node='DOTerm', prop='defLinks'), \
                            dict(node='DOTerm', prop='is_obsolete'), \
                            dict(node='DOTerm', prop='subset'), \
                            dict(node='DOTerm', prop='primaryKey'), \
                            dict(node='MITerm', prop='primaryKey'), \
                            dict(node='Identifier', prop='primaryKey'), \
                            dict(node='Synonym', prop='primaryKey'), \
                            dict(node='CrossReference', prop='localId'), \
                            dict(node='CrossReference', prop='name'), \
                            dict(node='CrossReference', prop='primaryKey'), \
                            dict(node='CrossReference', prop='prefix'), \
                            dict(node='CrossReference', prop='crossRefType'), \
                            dict(node='CrossReference', prop='displayName'), \
                            dict(node='CrossReference', prop='globalCrossRefId'), \
                            dict(node='CrossReference', prop='uuid'),\
                            dict(node='CrossReference', prop='page'), \
                            dict(node='Species', prop='name'), \
                            dict(node='Species', prop='species'), \
                            dict(node='Species', prop='primaryKey'), \
                            dict(node='Entity', prop='dateProduced'), \
                            dict(node='Entity', prop='primaryKey'), \
                            dict(node='Chromosome', prop='primaryKey'), \
                            dict(node='DiseaseEntityJoin', prop='primaryKey'), \
                            dict(node='DiseaseEntityJoin', prop='joinType'), \
                            dict(node='PhenotypeEntityJoin', prop='primaryKey'), \
                            dict(node='InteractionGeneJoin', prop='joinType'), \
                            dict(node='Association', prop='joinType'), \
                            dict(node='Association', prop='primaryKey'), \
                            dict(node='Phenotype', prop='primaryKey'), \
                            dict(node='Phenotype', prop='phenotypeStatement'), \
                            dict(node='Publication', prop='pubMedId'), \
                           #dict(node='Publication', prop='pubModId'), \
                            dict(node='Publication', prop='primaryKey'), \
                            dict(node='EvidenceCode', prop='primaryKey'), \
                            dict(node='Feature', prop='primaryKey'), \
                            dict(node='Feature', prop='symbol'), \
                            dict(node='Feature', prop='dateProduced'), \
                            dict(node='Feature', prop='uuid'), \
                            dict(node='Feature', prop='dataProvider'), \
                            dict(node='GOTerm', prop='definition'), \
                            dict(node='DOTerm', prop='definition'), \
                            dict(node='GOTerm', prop='type'), \
                            dict(node='DOTerm', prop='subset'), \
                            dict(node='ExpressionBioEntity', prop='primaryKey'), \
                            dict(node='BioEntityGeneExpressionJoin', prop='primaryKey'), \
                            dict(node='DOTerm', prop='defLinks')
                            ],

        'test_prop_not_null': [dict(node='Gene', prop='modGlobalCrossRefId'), \
                               dict(node='Gene', prop='dateProduced'), \
                               dict(node='Gene', prop='geneLiteratureUrl'), \
                               dict(node='Gene', prop='modCrossRefCompleteUrl'), \
                               dict(node='Gene', prop='taxonId'), \
                               dict(node='Gene', prop='geneticEntityExternalUrl'), \
                               dict(node='Gene', prop='modLocalId'), \
                               dict(node='Gene', prop='symbol'), \
                               dict(node='Gene', prop='primaryKey'), \
                               dict(node='Gene', prop='modGlobalId'), \
                               dict(node='Gene', prop='uuid'), \
                               dict(node='Gene', prop='dataProvider'), \
                               dict(node='Feature', prop='dataProvider'), \
                               dict(node='GOTerm', prop='primaryKey'), \
                               #dict(node='SOTerm', prop='name'), \
                               dict(node='SOTerm', prop='primaryKey'), \
                               dict(node='DOTerm', prop='doPrefix'), \
                               dict(node='DOTerm', prop='doId'), \
                               dict(node='DOTerm', prop='doDisplayId'), \
                               dict(node='DOTerm', prop='doUrl'), \
                               dict(node='DOTerm', prop='defLinks'), \
                               dict(node='DOTerm', prop='is_obsolete'), \
                               dict(node='DOTerm', prop='subset'), \
                               dict(node='DOTerm', prop='primaryKey'), \
                               dict(node='Identifier', prop='primaryKey'), \
                               dict(node='Synonym', prop='primaryKey'), \
                               dict(node='CrossReference', prop='localId'), \
                               dict(node='CrossReference', prop='name'), \
                               dict(node='CrossReference', prop='primaryKey'), \
                               dict(node='CrossReference', prop='prefix'), \
                               dict(node='CrossReference', prop='crossRefType'), \
                               dict(node='CrossReference', prop='displayName'), \
                               dict(node='CrossReference', prop='globalCrossRefId'), \
                               dict(node='CrossReference', prop='uuid'),\
                               dict(node='Species', prop='name'), \
                               dict(node='Species', prop='species'), \
                               dict(node='Species', prop='primaryKey'), \
                               dict(node='Entity', prop='dateProduced'), \
                               dict(node='Entity', prop='primaryKey'), \
                               dict(node='Chromosome', prop='primaryKey'), \
                               dict(node='DiseaseEntityJoin', prop='joinType'), \
                               dict(node='DiseaseEntityJoin', prop='primaryKey'), \
                               dict(node='DiseaseEntityJoin', prop='joinType'), \
                               dict(node='DiseaseEntityJoin', prop='primaryKey'), \
                               dict(node='PhenotypeEntityJoin', prop='primaryKey'), \
                               dict(node='Phenotype', prop='phenotypeStatement'), \
                               dict(node='Association', prop='joinType'), \
                               dict(node='Association', prop='primaryKey'), \
                               dict(node='Publication', prop='pubMedId'), \
                               dict(node='Publication', prop='primaryKey'), \
                               dict(node='EvidenceCode', prop='primaryKey'), \
                               dict(node='Feature', prop='primaryKey'), \
                               dict(node='Feature', prop='symbol'), \
                               dict(node='Feature', prop='dateProduced'), \
                               dict(node='Feature', prop='globalId'), \
                               dict(node='Feature', prop='uuid'), \
                               dict(node='MITerm', prop='primaryKey'), \
                               dict(node='ExpressionBioEntity', prop='primaryKey'),
                               dict(node='BioEntityGeneExpressionJoin', prop='primaryKey'), \
                               dict(node='Stage', prop='primaryKey'), \
                               ],

        'test_prop_unique': [dict(node='EvidenceCode', prop='primaryKey'), \
                             dict(node='Publication', prop='primaryKey'), \
                             dict(node='Association', prop='primaryKey'), \
                             dict(node='DiseaseEntityJoin', prop='primaryKey'), \
                             dict(node='PhenotypeEntityJoin', prop='primaryKey'), \
                             dict(node='Phenotype', prop='primaryKey'), \
                             dict(node='Chromosome', prop='primaryKey'), \
                             dict(node='Entity', prop='primaryKey'), \
                             dict(node='Species', prop='primaryKey'), \
                             dict(node='CrossReference', prop='primaryKey'), \
                             dict(node='CrossReference', prop='uuid'), \
                             dict(node='Synonym', prop='primaryKey'), \
                             dict(node='DOTerm', prop='primaryKey'), \
                             dict(node='SOTerm', prop='primaryKey'), \
                             dict(node='GOTerm', prop='primaryKey'), \
                             dict(node='Gene', prop='primaryKey'), \
                             dict(node='Gene', prop='uuid'), \
                             dict(node='Feature', prop='primaryKey'), \
                             dict(node='Feature', prop='uuid'), \
                             dict(node='MITerm', prop='primaryKey'),
                             dict(node='Stage', prop='primaryKey'), \
                             # with uberon, this can not be unique any longer, unless
                             # every term is just 'ontology' not ontology-specific node labels.
                             # dict(node='Ontology', prop='primaryKey'),
                             # TODO refactor ontology transaction to use id prefix to name node labels so
                             # we can turn this back on
                             dict(node='BioEntityGeneExpressionJoin', prop='primaryKey'),
                             dict(node='ExpressionBioEntity', prop='primaryKey')
                             ]
    }

    # Query to return all distinct properties from all nodes of a certain type:
    # MATCH (n:Gene) WITH DISTINCT keys(n) AS keys UNWIND keys AS keyslisting WITH DISTINCT keyslisting AS allfields RETURN allfields;

    def test_node_exists(self, node):
        query = 'MATCH (n:%s) RETURN DISTINCT COUNT(n) as count' % (node)

        result = execute_transaction(query)
        for record in result:
            assert record["count"] > 0

    def test_prop_exist(self, node, prop):
        query = 'MATCH (n:%s) WHERE NOT EXISTS(n.%s) RETURN COUNT(n) as count' % (node, prop)

        result = execute_transaction(query)
        for record in result:
            assert record["count"] == 0

    def test_prop_not_null(self, node, prop):
        query = 'MATCH (n:%s) WHERE n.%s is NULL RETURN COUNT(n) as count' % (node, prop)

        result = execute_transaction(query)
        for record in result:
            assert record["count"] == 0

    def test_prop_unique(self, node, prop):
        query = 'MATCH (n:%s) WITH n.%s AS value, COLLECT(n) AS nodelist, COUNT(*) AS count RETURN count' % (node, prop)

        result = execute_transaction(query)
        for record in result:
            assert record["count"] == 1