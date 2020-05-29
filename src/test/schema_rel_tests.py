"""Test Class"""

from etl import Neo4jHelper


def pytest_generate_tests(metafunc):
    """called once per each test function"""
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(argnames, [[funcargs[name] for name in argnames] \
                                     for funcargs in funcarglist])


class TestClass():
    """Test Class"""

    # a map specifying multiple argument sets for a test method
    # test_rel_exists checks for the existence of *at least one count*
    # of the relationship specified below.
    params = {
        'test_rel_exists': [dict(node1='Ontology:SOTerm', node2='Gene'),
                            dict(node1='Ontology:DOTerm', node2='Identifier:CrossReference'),
                            dict(node1='Ontology:DOTerm', node2='Ontology:DOTerm'),
                            dict(node1='Ontology:DOTerm', node2='Identifier:Synonym'),
                            dict(node1='Ontology:DOTerm', node2='DiseaseEntityJoin:Association'),
                            dict(node1='Identifier:Synonym', node2='Ontology:DOTerm'),
                            dict(node1='Identifier:CrossReference', node2='Ontology:DOTerm'),
                            dict(node1='Ontology', node2='SecondaryId'),
                            dict(node1='Ontology:OBITerm', node2='Ontology:OBITerm'),
                            dict(node1='Ontology:BTOTerm', node2='Ontology:BTOTerm'),
                            dict(node1='Ontology:CHEBITerm', node2='Ontology:CHEBITerm'),
                            dict(node1='Gene', node2='Identifier:Synonym'),
                            dict(node1='Gene', node2='Identifier:SecondaryId'),
                            dict(node1='Gene', node2='CrossReference'),
                            dict(node1='Gene', node2='Species'),
                            dict(node1='Load', node2='Gene'),
                            dict(node1='Allele', node2='Species'),
                            dict(node1='Allele', node2='Synonym'),
                            dict(node1='Gene', node2='Ontology:GOTerm'),
                            dict(node1='Gene', node2='Ontology:SOTerm'),
                            dict(node1='Gene', node2='Chromosome'),
                            dict(node1='Gene', node2='DiseaseEntityJoin:Association'),
                            dict(node1='GenomicLocation', node2='Assembly'),
                            dict(node1='GenomicLocation', node2='Chromosome'),
                            dict(node1='GenomicLocation', node2='Variant'),
                            dict(node1='GenomicLocation', node2='Gene'),
                            dict(node1='Exon', node2='GenomicLocation'),
                            dict(node1='Transcript', node2='GenomicLocation'),
                            dict(node1='Identifier:SecondaryId', node2='Gene'),
                            dict(node1='Identifier:Synonym', node2='Gene'),
                            dict(node1='Species', node2='Gene'),
                            dict(node1='CrossReference', node2='Gene'),
                            dict(node1='Chromosome', node2='Gene'),
                            dict(node1='DiseaseEntityJoin:Association', node2='Gene'),
                            dict(node1='DiseaseEntityJoin:Association', node2='Ontology:DOTerm'),
                            dict(node1='PublicationJoin:Association', node2='Publication'),
                            dict(node1='PublicationJoin:Association', node2='ECOTerm'),
                            dict(node1='PublicationJoin:Association',
                                 node2='DiseaseEntityJoin:Association'),
                            dict(node1='InteractionGeneJoin:Association', node2='Gene'),
                            dict(node1='InteractionGeneJoin:Association', node2='Ontology:MITerm'),
                            dict(node1='InteractionGeneJoin:Association', node2='Publication'),
                            dict(node1='InteractionGeneJoin:Association',
                                 node2='Identifier:CrossReference'),
                            dict(node1='Allele', node2='CrossReference'),
                            dict(node1='PhenotypeEntityJoin:Association', node2='Phenotype'),
                            dict(node1='PhenotypeEntityJoin:Association', node2='Gene'),
                            dict(node1='PhenotypeEntityJoin:Association', node2='Allele'),
                            dict(node1='Gene', node2='ExpressionBioEntity'),
                            dict(node1='Gene', node2='BioEntityGeneExpressionJoin'),
                            dict(node1='BioEntityGeneExpressionJoin', node2='Stage'),
                            dict(node1='BioEntityGeneExpressionJoin', node2='Publication'),
                            dict(node1='BioEntityGeneExpressionJoin', node2='Ontology'),
                            dict(node1='BioEntityGeneExpressionJoin', node2='MMOTerm'),
                            dict(node1='ExpressionBioEntity', node2='GOTerm'),
                            dict(node1='ExpressionBioEntity', node2='Ontology'),
                            dict(node1='ExpressionBioEntity', node2='ZFATerm'),
                            dict(node1='Variant', node2='Chromosome'),
                            dict(node1='Ontology', node2='Ontology'),
                            dict(node1='SequenceTargetingReagent', node2='Gene'),
                            dict(node1='AffectedGenomicModel', node2='Species'),
                            dict(node1='AffectedGenomicModel', node2='Feature'),
                            dict(node1='AffectedGenomicModel', node2='AffectedGenomicModel'),
                            dict(node1='Variant', node2='SOTerm'),
                            dict(node1='Variant', node2='Feature'),
                            dict(node1='Variant', node2='Synonym'),
                            dict(node1='AffectedGenomicModel', node2='SequenceTargetingReagent'),
                            dict(node1='AffectedGenomicModel', node2='AffectedGenomicModel'),
                            dict(node1='AffectedGenomicModel', node2='Feature'),
                            dict(node1='AffectedGenomicModel', node2='DiseaseEntityJoin'),
                            dict(node1='AffectedGenomicModel', node2='PhenotypeEntityJoin'),
                            dict(node1='AffectedGenomicModel', node2='PublicationJoin:Association'),
                            dict(node1='DiseaseEntityJoin', node2='CrossReference'),
                            dict(node1='PublicationJoin', node2='AffectedGenomicModel'),
                            dict(node1='PublicationJoin', node2='Allele'),
                            dict(node1='GeneLevelConsequence', node2='Gene'),
                            dict(node1='GeneLevelConsequence', node2='Variant')
                            ]

    }

    @staticmethod
    def test_rel_exists(node1, node2):
        """Test Relationship Exists"""

        query = """MATCH (n:%s)-[]-(m:%s)
                   RETURN DISTINCT COUNT(n) AS count""" % (node1, node2)

        result = Neo4jHelper.run_single_query(query)
        for record in result:
            assert record["count"] > 0
