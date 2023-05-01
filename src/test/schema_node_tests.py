"""Schema Node Tests"""

from etl import Neo4jHelper


def pytest_generate_tests(metafunc):
    """PyTest Generate Tests"""

    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(argnames, [[funcargs[name] for name in argnames]
                                    for funcargs in funcarglist])


class TestClass():
    """A map specifying multiple argument sets for a test method"""

    params = {
        'test_relationship_exists': [dict(relationship='IS_A_PART_OF_CLOSURE'),
                                     dict(relationship='LOCATED_ON'),
                                     dict(relationship='VARIATION'),
                                     dict(relationship='PART_OF'),
                                     dict(relationship='MODEL_COMPONENT'),
                                     dict(relationship='BACKGROUND'),
                                     dict(relationship='ANNOTATION_SOURCE_CROSS_REFERENCE'),
                                     dict(relationship='PRIMARY_GENETIC_ENTITY'),
                                     dict(relationship='EXPRESSES'),
                                     dict(relationship='IS_REGULATED_BY'),
                                     dict(relationship='COMPUTED_GENE'),
                                     dict(relationship='CONTAINS'),
                                     dict(relationship='TRANSCRIPT'),
                                     dict(relationship='IS_MODEL_OF'),
                                     dict(relationship='IS_IMPLICATED_IN'),
                                     dict(relationship='IS_MARKER_FOR'),
                                     dict(relationship='IS_NOT_MARKER_FOR'),
                                     dict(relationship='ASSOCIATION'),
                                     ],

        'test_node_exists': [dict(node='Ontology'),
                             dict(node='Variant'),
                             dict(node='SOTerm'),
                             dict(node='DOTerm'),
                             dict(node='GOTerm'),
                             dict(node='MITerm'),
                             dict(node='Identifier'),
                             dict(node='Gene'),
                             dict(node='Synonym'),
                             dict(node='CrossReference'),
                             dict(node='Construct'),
                             dict(node='Species'),
                             dict(node='Entity'),
                             dict(node='Chromosome'),
                             dict(node='DiseaseEntityJoin'),
                             dict(node='HTPDataset'),
                             dict(node='HTPDatasetSample'),
                             dict(node='CategoryTag'),
                             dict(node='Association'),
                             dict(node='Publication'),
                             dict(node='Allele'),
                             dict(node='AffectedGenomicModel'),
                             dict(node='Phenotype'),
                             dict(node='PhenotypeEntityJoin'),
                             dict(node='OrthologyGeneJoin'),
                             dict(node='OrthoAlgorithm'),
                             dict(node='Load'),
                             dict(node='ExpressionBioEntity'),
                             dict(node='Stage'),
                             dict(node='SequenceTargetingReagent'),
                             dict(node='BioEntityGeneExpressionJoin'),
                             dict(node='InteractionGeneJoin'),
                             dict(node='ZFATerm'),
                             dict(node='WBBTTerm'),
                             dict(node='CLTerm'),
                             dict(node='UBERONTerm'),
                             dict(node='FBCVTerm'),
                             dict(node='FBBTTerm'),
                             dict(node='MATerm'),
                             dict(node='EMAPATerm'),
                             dict(node='MMUSDVTerm'),
                             dict(node='BSPOTerm'),
                             dict(node='WBLSTerm'),
                             dict(node='OBITerm'),
                             dict(node='BTOTerm'),
                             dict(node='CHEBITerm'),
                             dict(node='ZECOTerm'),
                             dict(node='Assembly'),
                             dict(node='GenomicLocation'),
                             dict(node='PublicationJoin'),
                             dict(node='GeneLevelConsequence'),
                             dict(node='Transcript'),
                             dict(node='Exon'),
                             dict(node='TranscriptProteinSequence'),
                             dict(node='VariantProteinSequence'),
                             dict(node='CDSSequence'),
                             dict(node='ExperimentalCondition')
                             ],

        'test_prop_exist': [dict(node='Construct', prop='primaryKey'),
                            dict(node='Construct', prop='name'),
                            dict(node='Construct', prop='nameText'),
                            dict(node='Construct', prop='symbol'),
                            dict(node='Gene', prop='modGlobalCrossRefId'),
                            dict(node='Gene', prop='geneLiteratureUrl'),
                            dict(node='Gene', prop='modCrossRefCompleteUrl'),
                            dict(node='Gene', prop='taxonId'),
                            dict(node='Gene', prop='geneticEntityExternalUrl'),
                            dict(node='Gene', prop='modLocalId'),
                            dict(node='Gene', prop='symbol'),
                            dict(node='Gene', prop='primaryKey'),
                            dict(node='Gene', prop='modGlobalId'),
                            dict(node='Gene', prop='uuid'),
                            dict(node='Gene', prop='symbolWithSpecies'),
                            dict(node='GOTerm', prop='primaryKey'),
                            dict(node='Gene', prop='dataProvider'),
                            dict(node='AffectedGenomicModel', prop='primaryKey'),
                            dict(node='AffectedGenomicModel', prop='name'),
                            dict(node='AffectedGenomicModel', prop='nameText'),
                            dict(node='AffectedGenomicModel', prop='nameTextWithSpecies'),
                            dict(node='AffectedGenomicModel', prop='nameWithSpecies'),
                            dict(node='SOTerm', prop='primaryKey'),
                            dict(node='DOTerm', prop='doPrefix'),
                            dict(node='DOTerm', prop='doId'),
                            dict(node='DOTerm', prop='doDisplayId'),
                            dict(node='DOTerm', prop='doUrl'),
                            dict(node='DOTerm', prop='defLinks'),
                            dict(node='DOTerm', prop='isObsolete'),
                            dict(node='DOTerm', prop='subset'),
                            dict(node='DOTerm', prop='primaryKey'),
                            dict(node='MITerm', prop='primaryKey'),
                            dict(node='CHEBITerm', prop='primaryKey'),
                            dict(node='ZECOTerm', prop='primaryKey'),
                            dict(node='TranscriptProteinSequence', prop='primaryKey'),
                            dict(node='TranscriptProteinSequence', prop='proteinSequence'),
                            dict(node='VariantProteinSequence', prop='primaryKey'),
                            dict(node='VariantProteinSequence', prop='proteinSequence'),
                            dict(node='CDSSequence', prop='primaryKey'),
                            dict(node='Identifier', prop='primaryKey'),
                            dict(node='Synonym', prop='primaryKey'),
                            dict(node='SequenceTargetingReagent', prop='primaryKey'),
                            dict(node='CrossReference', prop='localId'),
                            dict(node='CrossReference', prop='name'),
                            dict(node='CrossReference', prop='primaryKey'),
                            dict(node='CrossReference', prop='prefix'),
                            dict(node='CrossReference', prop='crossRefType'),
                            dict(node='CrossReference', prop='displayName'),
                            dict(node='CrossReference', prop='globalCrossRefId'),
                            dict(node='CrossReference', prop='uuid'),
                            dict(node='CrossReference', prop='page'),
                            dict(node='Species', prop='name'),
                            dict(node='Species', prop='species'),
                            dict(node='Species', prop='primaryKey'),
                            dict(node='Species', prop='phylogeneticOrder'),
                            dict(node='Species', prop='dataProviderFullName'),
                            dict(node='Species', prop='dataProviderShortName'),
                            dict(node='Species', prop='commonNames'),
                            dict(node='Chromosome', prop='primaryKey'),
                            dict(node='Entity', prop='primaryKey'),
                            dict(node='DiseaseEntityJoin', prop='primaryKey'),
                            dict(node='DiseaseEntityJoin', prop='joinType'),
                            dict(node='DiseaseEntityJoin', prop='sortOrder'),
                            dict(node='ExperimentalCondition', prop='primaryKey'),
                            dict(node='ExperimentalCondition', prop='conditionClassId'),
                            dict(node='ExperimentalCondition', prop='conditionId'),
                            dict(node='ExperimentalCondition', prop='anatomicalOntologyId'),
                            dict(node='ExperimentalCondition', prop='chemicalOntologyId'),
                            dict(node='ExperimentalCondition', prop='geneOntologyId'),
                            dict(node='ExperimentalCondition', prop='NCBITaxonID'),
                            dict(node='ExperimentalCondition', prop='conditionStatement'),
                            dict(node='PhenotypeEntityJoin', prop='primaryKey'),
                            dict(node='InteractionGeneJoin', prop='joinType'),
                            dict(node='Association', prop='joinType'),
                            dict(node='Association', prop='primaryKey'),
                            dict(node='Phenotype', prop='primaryKey'),
                            dict(node='Phenotype', prop='phenotypeStatement'),
                            dict(node='Publication', prop='primaryKey'),
                            dict(node='Allele', prop='primaryKey'),
                            dict(node='Allele', prop='symbol'),
                            dict(node='Allele', prop='uuid'),
                            dict(node='Allele', prop='dataProvider'),
                            dict(node='Allele', prop='symbolText'),
                            dict(node='Allele', prop='symbolTextWithSpecies'),
                            dict(node='Allele', prop='symbolWithSpecies'),
                            dict(node='GOTerm', prop='definition'),
                            dict(node='DOTerm', prop='definition'),
                            dict(node='GOTerm', prop='type'),
                            dict(node='DOTerm', prop='subset'),
                            dict(node='ExpressionBioEntity', prop='primaryKey'),
                            dict(node='ExpressionBioEntity', prop='whereExpressedStatement'),
                            dict(node='BioEntityGeneExpressionJoin', prop='primaryKey'),
                            dict(node='DOTerm', prop='defLinks'),
                            dict(node='Variant', prop='primaryKey'),
                            dict(node='Assembly', prop='primaryKey'),
                            dict(node='GenomicLocation', prop='chromosome'),
                            dict(node='GenomicLocation', prop='assembly'),
                            dict(node='GeneLevelConsequence', prop='geneLevelConsequence'),
                            dict(node='TranscriptLevelConsequence', prop='aminoAcidReference'),
                            dict(node='TranscriptLevelConsequence', prop='aminoAcidVariation'),
                            dict(node='TranscriptLevelConsequence', prop='aminoAcidChange'),
                            dict(node='TranscriptLevelConsequence', prop='cdnaStartPosition'),
                            dict(node='TranscriptLevelConsequence', prop='cdnaEndPosition'),
                            dict(node='TranscriptLevelConsequence', prop='cdnaRange'),
                            dict(node='TranscriptLevelConsequence', prop='cdsStartPosition'),
                            dict(node='TranscriptLevelConsequence', prop='cdsEndPosition'),
                            dict(node='TranscriptLevelConsequence', prop='cdsRange'),
                            dict(node='TranscriptLevelConsequence', prop='codonReference'),
                            dict(node='TranscriptLevelConsequence', prop='codonVariation'),
                            dict(node='TranscriptLevelConsequence', prop='codonChange'),
                            dict(node='TranscriptLevelConsequence', prop='proteinStartPosition'),
                            dict(node='TranscriptLevelConsequence', prop='proteinEndPosition'),
                            dict(node='TranscriptLevelConsequence', prop='proteinRange'),
                            dict(node='Transcript', prop='primaryKey'),
                            dict(node='Transcript', prop='gff3ID'),
                            dict(node='Transcript', prop='name')
                            ],


        'test_prop_not_null': [dict(node='AffectedGenomicModel', prop='primaryKey'),
                               dict(node='AffectedGenomicModel', prop='name'),
                               dict(node='Gene', prop='modGlobalCrossRefId'),
                               dict(node='Variant', prop='primaryKey'),
                               dict(node='Gene', prop='geneLiteratureUrl'),
                               dict(node='Gene', prop='modCrossRefCompleteUrl'),
                               dict(node='Gene', prop='taxonId'),
                               dict(node='Gene', prop='geneticEntityExternalUrl'),
                               dict(node='Gene', prop='modLocalId'),
                               dict(node='Gene', prop='symbol'),
                               dict(node='Gene', prop='primaryKey'),
                               dict(node='Gene', prop='modGlobalId'),
                               dict(node='Gene', prop='uuid'),
                               dict(node='Gene', prop='dataProvider'),
                               dict(node='GOTerm', prop='primaryKey'),
                               dict(node='SOTerm', prop='primaryKey'),
                               dict(node='DOTerm', prop='doPrefix'),
                               dict(node='DOTerm', prop='doId'),
                               dict(node='DOTerm', prop='doDisplayId'),
                               dict(node='DOTerm', prop='doUrl'),
                               dict(node='DOTerm', prop='defLinks'),
                               dict(node='DOTerm', prop='isObsolete'),
                               dict(node='DOTerm', prop='subset'),
                               dict(node='DOTerm', prop='primaryKey'),
                               dict(node='CHEBITerm', prop='primaryKey'),
                               dict(node='ZECOTerm', prop='primaryKey'),
                               dict(node='Identifier', prop='primaryKey'),
                               dict(node='Synonym', prop='primaryKey'),
                               dict(node='Construct', prop='primaryKey'),
                               dict(node='Construct', prop='name'),
                               dict(node='Construct', prop='nameText'),
                               dict(node='Construct', prop='symbol'),
                               dict(node='CrossReference', prop='localId'),
                               dict(node='CrossReference', prop='name'),
                               dict(node='CrossReference', prop='primaryKey'),
                               dict(node='CrossReference', prop='prefix'),
                               dict(node='CrossReference', prop='crossRefType'),
                               dict(node='CrossReference', prop='displayName'),
                               dict(node='CrossReference', prop='globalCrossRefId'),
                               dict(node='CrossReference', prop='uuid'),
                               dict(node='Species', prop='name'),
                               dict(node='Species', prop='species'),
                               dict(node='Species', prop='primaryKey'),
                               dict(node='Entity', prop='primaryKey'),
                               dict(node='SequenceTargetingReagent', prop='primaryKey'),
                               dict(node='Chromosome', prop='primaryKey'),
                               dict(node='DiseaseEntityJoin', prop='joinType'),
                               dict(node='DiseaseEntityJoin', prop='primaryKey'),
                               dict(node='DiseaseEntityJoin', prop='joinType'),
                               dict(node='DiseaseEntityJoin', prop='primaryKey'),
                               dict(node='ExperimentalCondition', prop='primaryKey'),
                               dict(node='ExperimentalCondition', prop='conditionClassId'),
                               dict(node='PhenotypeEntityJoin', prop='primaryKey'),
                               dict(node='Phenotype', prop='phenotypeStatement'),
                               dict(node='Association', prop='joinType'),
                               dict(node='Association', prop='primaryKey'),
                               dict(node='Publication', prop='primaryKey'),
                               dict(node='Allele', prop='primaryKey'),
                               dict(node='Allele', prop='symbol'),
                               dict(node='Allele', prop='dataProvider'),
                               dict(node='Allele', prop='globalId'),
                               dict(node='Allele', prop='uuid'),
                               dict(node='Allele', prop='symbolText'),
                               dict(node='Allele', prop='symbolWithSpecies'),
                               dict(node='MITerm', prop='primaryKey'),
                               dict(node='ExpressionBioEntity', prop='primaryKey'),
                               dict(node='ExpressionBioEntity', prop='whereExpressedStatement'),
                               dict(node='BioEntityGeneExpressionJoin', prop='primaryKey'),
                               dict(node='Stage', prop='primaryKey'),
                               dict(node='Variant', prop='hgvsNomenclature'),
                               dict(node='Assembly', prop='primaryKey'),
                               dict(node='Assembly', prop='primaryKey'),
                               dict(node='GenomicLocation', prop='chromosome'),
                               dict(node='GenomicLocation', prop='assembly'),
                               dict(node='PublicationJoin', prop='primaryKey'),
                               dict(node='HTPDataset', prop='primaryKey'),
                               dict(node='HTPDatasetSample', prop='primaryKey'),
                               ],

        'test_prop_unique': [dict(node='Publication', prop='primaryKey'),
                             dict(node='Association', prop='primaryKey'),
                             dict(node='Variant', prop='primaryKey'),
                             dict(node='DiseaseEntityJoin', prop='primaryKey'),
                             dict(node='ExperimentalCondition', prop='primaryKey'),
                             dict(node='PhenotypeEntityJoin', prop='primaryKey'),
                             dict(node='Entity', prop='primaryKey'),
                             dict(node='Species', prop='primaryKey'),
                             dict(node='CrossReference', prop='primaryKey'),
                             dict(node='CrossReference', prop='uuid'),
                             dict(node='DOTerm', prop='primaryKey'),
                             dict(node='SOTerm', prop='primaryKey'),
                             dict(node='GOTerm', prop='primaryKey'),
                             dict(node='CHEBITerm', prop='primaryKey'),
                             dict(node='ZECOTerm', prop='primaryKey'),
                             dict(node='Gene', prop='primaryKey'),
                             dict(node='Gene', prop='uuid'),
                             dict(node='Allele', prop='primaryKey'),
                             dict(node='Allele', prop='uuid'),
                             dict(node='MITerm', prop='primaryKey'),
                             dict(node='Stage', prop='primaryKey'),
                             dict(node='SequenceTargetingReagent', prop='primaryKey'),
                             dict(node='AffectedGenomicModel', prop='primaryKey'),
                             dict(node='Variant', prop='hgvsNomenclature'),
                             dict(node='BioEntityGeneExpressionJoin', prop='primaryKey'),
                             dict(node='ExpressionBioEntity', prop='primaryKey'),
                             dict(node='HTPDataset', prop='primaryKey'),
                             dict(node='HTPDatasetSample', prop='primaryKey'),
                             dict(node='CategoryTag', prop='primaryKey'),
                             ]
    }


    @staticmethod
    def test_node_exists(node):
        """Test Node Exists"""

        query = """MATCH (n:%s)
                   RETURN DISTINCT COUNT(n) AS count""" % node
        with Neo4jHelper.run_single_query(query) as result:
            for record in result:
                assert record["count"] > 0

    @staticmethod
    def test_relationship_exists(relationship):
        """Test Relationship Exists"""

        query = """MATCH ()-[r:%s]-()
                   RETURN count(r) AS count""" % relationship

        with Neo4jHelper.run_single_query(query) as result:
            for record in result:
                assert record["count"] > 0

    @staticmethod
    def test_prop_exist(node, prop):
        """Test Prop Exits"""
        query = """MATCH (n:%s)
                   WHERE (n.%s) is NOT NULL
                   RETURN COUNT(n) AS count""" % (node, prop)

        with Neo4jHelper.run_single_query(query) as result:
            for record in result:
                assert record["count"] > 0

    @staticmethod
    def test_prop_not_null(node, prop):
        """Test Prop Not Null"""

        query = """MATCH (n:%s)
                   WHERE n.%s is NULL
                   RETURN COUNT(n) AS count""" % (node, prop)

        with Neo4jHelper.run_single_query(query) as result:
            for record in result:
                assert record["count"] == 0

    @staticmethod
    def test_prop_unique(node, prop):
        """Test Prop Unique"""

        query = """MATCH (n:%s)
                   WITH n.%s AS value, COLLECT(n) AS nodelist, COUNT(*) AS count
                   RETURN count""" % (node, prop)

        with Neo4jHelper.run_single_query(query) as result:
            for record in result:
                assert record["count"] == 1
