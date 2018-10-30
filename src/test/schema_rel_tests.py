from transactions import Transaction
import os

def pytest_generate_tests(metafunc):
    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(argnames, [[funcargs[name] for name in argnames]
            for funcargs in funcarglist])

# MATCH (n)-[r]-(m) RETURN DISTINCT labels(n), labels(m);


class TestClass(object):
    # a map specifying multiple argument sets for a test method
    # test_rel_exists checks for the existence of *at least one count* of the relationship specified below.
    params = {
        'test_rel_exists': [dict(node1='Ontology:SOTerm', node2='Gene'),
                            dict(node1='Ontology:DOTerm', node2='Identifier:CrossReference'),
                            dict(node1='Ontology:DOTerm', node2='Ontology:DOTerm'),
                            dict(node1='Ontology:DOTerm', node2='Identifier:Synonym'),
                            dict(node1='Ontology:DOTerm', node2='DiseaseEntityJoin:Association'),
                            dict(node1='Identifier:Synonym', node2='Ontology:DOTerm'),
                            dict(node1='Identifier:CrossReference', node2='Ontology:DOTerm'),
                            dict(node1='Gene', node2='Identifier:Synonym'),
                            dict(node1='Gene', node2='Identifier:SecondaryId'),
                            dict(node1='Gene', node2='CrossReference'),
                            dict(node1='Gene', node2='Species'),
                            dict(node1='Load', node2='Gene'),
                            dict(node1='Feature', node2='Species'),
                            dict(node1='Feature', node2='SecondaryId'),
                            dict(node1='Feature', node2='Synonym'),
                            dict(node1='Gene', node2='Ontology:GOTerm'),
                            dict(node1='Gene', node2='Ontology:SOTerm'),
                            dict(node1='Gene', node2='Entity'),
                            dict(node1='Feature', node2='Entity'),
                            dict(node1='Gene', node2='Chromosome'),
                            dict(node1='Gene', node2='DiseaseEntityJoin:Association'),
                            dict(node1='Identifier:SecondaryId', node2='Gene'),
                            dict(node1='Identifier:Synonym', node2='Gene'),
                            dict(node1='Species', node2='Gene'),
                            dict(node1='Entity', node2='Gene'),
                            dict(node1='CrossReference', node2='Gene'),
                            dict(node1='Chromosome', node2='Gene'),
                            dict(node1='DiseaseEntityJoin:Association', node2='Gene'),
                            dict(node1='DiseaseEntityJoin:Association', node2='Ontology:DOTerm'),
                            dict(node1='DiseaseEntityJoin:Association', node2='Publication'),
                            dict(node1='DiseaseEntityJoin:Association', node2='EvidenceCode'),
                            dict(node1='DiseaseEntityJoin:Association', node2='Ontology:DOTerm'),
                            dict(node1='InteractionGeneJoin:Association', node2='Gene'),
                            dict(node1='InteractionGeneJoin:Association', node2='Ontology:MITerm'),
                            dict(node1='InteractionGeneJoin:Association', node2='Publication'),
                            dict(node1='InteractionGeneJoin:Association', node2='Identifier:CrossReference'),
                            dict(node1='Feature', node2='CrossReference'),
                            dict(node1='PhenotypeEntityJoin:Association', node2='Publication'),
                            dict(node1='Gene', node2='ExpressionBioEntity'),
                            dict(node1='Gene', node2='BioEntityGeneExpressionJoin'),
                            dict(node1='BioEntityGeneExpressionJoin', node2='Stage'),
                            dict(node1='BioEntityGeneExpressionJoin', node2='Publication'),
                            dict(node1='BioEntityGeneExpressionJoin', node2='Ontology'),
                            dict(node1='BioEntityGeneExpressionJoin', node2='MMOTerm'),
                            dict(node1='ExpressionBioEntity', node2='GOTerm'),
                            dict(node1='ExpressionBioEntity', node2='Ontology'),
                            dict(node1='ExpressionBioEntity', node2='ZFATerm')
                            ]

    }

    def test_rel_exists(self, node1, node2):
        query = 'MATCH (n:%s)-[]-(m:%s) RETURN DISTINCT COUNT(n) as count' % (node1, node2)

        result = Transaction.run_single_query(query)
        for record in result:
            assert record["count"] > 0
