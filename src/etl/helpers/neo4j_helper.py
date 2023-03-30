"""Neo4j Helper"""

import logging
from contextlib import contextmanager

from neo4j import GraphDatabase
from loader_common import ContextInfo

logger = logging.getLogger(__name__)
context_info = ContextInfo()
uri = "bolt://" + context_info.env["NEO4J_HOST"] + ":" + str(context_info.env["NEO4J_PORT"])

class Neo4jHelper:
    """Neo4j Helper"""

    @staticmethod
    @contextmanager
    def run_single_parameter_query(query, parameter):
        """Run single parameter query"""
        try:
            graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1, max_connection_lifetime=3600)
            logger.debug("Running run_single_parameter_query. Please wait...")
            logger.debug("Query: %s", query)
            with graph.session() as session:
                with session.begin_transaction() as transaction:
                    yield transaction.run(query, parameter=parameter)
        finally:
            logger.debug("closing neo4j transaction and session")
            graph.close()

    @staticmethod
    @contextmanager
    def run_single_query(query):
        """Run Single Query"""
        try:
            graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1, max_connection_lifetime=3600)
            with graph.session() as session:
                with session.begin_transaction() as transaction:
                    yield transaction.run(query)
        finally:
            logger.debug("closing neo4j transaction and session")
            graph.close()

    @staticmethod
    def run_single_query_no_return(query):
        """Run Single Query"""
        graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1, max_connection_lifetime=3600)
        with graph.session() as session:
            with session.begin_transaction() as transaction:
                transaction.run(query)
        graph.close()

    @staticmethod
    def create_indices():
        """Create Indicies"""
        graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1, max_connection_lifetime=3600)
        with graph.session() as session:

            constraints =  [['n:Publication', 'n.primaryKey'],
                    ['n:Association', 'n.primaryKey'],
                    ['n:Variant', 'n.primaryKey'],
                    ['n:DiseaseEntityJoin', 'n.primaryKey'],
                    ['n:ExperimentalCondition', 'n.primaryKey'],
                    # ['n:PhenotypeEntityJoin', 'n.primaryKey'], # Breaks MERGE in Phenotype ETL
                    # ['n:Entity', 'n.primaryKey'], # Breaks MERGE in BGI ETL
                    ['n:Species', 'n.primaryKey'],
                    ['n:CrossReference', 'n.primaryKey'],
                    ['n:CrossReference', 'n.uuid'],
                    ['n:Gene', 'n.primaryKey'],
                    ['n:Gene', 'n.uuid'],
                    ['n:Allele', 'n.primaryKey'],
                    ['n:Allele', 'n.uuid'],
                    ['n:Stage', 'n.primaryKey'],
                    ['n:SequenceTargetingReagent', 'n.primaryKey'],
                    ['n:AffectedGenomicModel', 'n.primaryKey'],
                    ['n:Variant', 'n.hgvsNomenclature'],
                    # ['n:BioEntityGeneExpressionJoin', 'n.primaryKey'], # Breaks MERGE in Expression ETL
                    # ['n:ExpressionBioEntity', 'n.primaryKey'], # Breaks MERGE in Expression ETL
                    ['n:HTPDataset', 'n.primaryKey'],
                    ['n:HTPDatasetSample', 'n.primaryKey'],
                    ['n:CategoryTag', 'n.primaryKey'],
                    ['n:CHEBITerm', 'n.primaryKey'],
                    ['n:ZECOTerm', 'n.primaryKey'],
                    ['n:DOTerm', 'n.primaryKey'],
                    ['n:SOTerm', 'n.primaryKey'],
                    ['n:GOTerm', 'n.primaryKey'],
                    ['n:MITerm', 'n.primaryKey'],
                    ["n:ECOTerm", "n.primaryKey"],
                    ["n:ZFATerm", "n.primaryKey"],
                    ["n:ZFSTerm", "n.primaryKey"],
                    ["n:CLTerm", "n.primaryKey"],
                    ["n:WBBTTerm", "n.primaryKey"],
                    ["n:FBCVTerm", "n.primaryKey"],
                    ["n:FBBTTerm", "n.primaryKey"],
                    ["n:MATerm", "n.primaryKey"],
                    ["n:EMAPATerm", "n.primaryKey"],
                    ["n:UBERONTerm", "n.primaryKey"],
                    ["n:PATOTerm", "n.primaryKey"],
                    ["n:APOTerm", "n.primaryKey"],
                    ["n:DPOTerm", "n.primaryKey"],
                    ["n:FYPOTerm", "n.primaryKey"],
                    ["n:WBPhenotypeTerm", "n.primaryKey"],
                    ["n:MPTerm", "n.primaryKey"],
                    ["n:HPTerm", "n.primaryKey"],
                    ["n:OBITerm", "n.primaryKey"],
                    ["n:BTOTerm", "n.primaryKey"],
                    ["n:MMUSDVTerm", "n.primaryKey"],
                    ["n:BSPOTerm", "n.primaryKey"],
                    ["n:MMOTerm", "n.primaryKey"],
                    ["n:WBLSTerm", "n.primaryKey"],
                    ["n:XPOTerm", "n.primaryKey"],
                    ["n:XSMOTerm", "n.primaryKey"],
                    ["n:XAOTerm", "n.primaryKey"],
                    ["n:XBEDTerm", "n.primaryKey"],
                    ["n:NonBGIConstructComponent", "n.primaryKey"],
                    ['n:Exon', 'n.primaryKey'], 
                    ['n:Transcript', 'n.primaryKey'],
                    ['n:CDS', 'n.primaryKey'], 
                    ['n:GenomicLocation', 'n.primaryKey'],
                    ['n:OrthoAlgorithm', 'n.name']
                    ]

            # Constraints must be run before indices.
            for constraint in constraints:
                session.run("CREATE CONSTRAINT FOR ({}) REQUIRE {} IS UNIQUE".format(constraint[0], constraint[1]))


            indicies = ["(n:CDS) on (n.primaryKey)",
                        "(n:Gene) on (n.primaryKey)",
                        "(n:Gene) on (n.modLocalId)",
                        "(n:Gene) on (n.symbol)",
                        "(n:Gene) on (n.gff3ID)",
                        "(n:Gene) on (n.taxonId)",
                        "(n:Construct) on (n.primaryKey)",
                        "(n:Transcript) on (n.primaryKey)",
                        "(n:Transcript) on (n.dataProvider)",
                        "(n:TranscriptLevelConsequence) on (n.primaryKey)",
                        "(n:TranscriptProteinSequence) on (n.primaryKey)",
                        "(n:GeneLevelConsequence) on (n.primaryKey)",
                        "(n:Transcript) on (n.gff3ID)",
                        "(n:CDS) on (n.gff3ID)",
                        "(n:GOTerm) on (n.primaryKey)",
                        "(n:Genotype) on (n.primaryKey)",
                        "(n:AffectedGenomicModel) on (n.primaryKey)",
                        "(n:SOTerm) on (n.primaryKey)",
                        "(n:SOTerm) on (n.name)",
                        "(n:Ontology) on (n.primaryKey)",
                        "(n:Ontology) on (n.name)",
                        "(n:DOTerm) on (n.primaryKey)",
                        "(n:DOTerm) on (n.oid)",
                        "(n:GOTerm) on (n.oid)",
                        "(n:GenomicLocation) on (n.primaryKey)",
                        "(n:Assembly) on (n.primaryKey)",
                        "(n:Publication) on (n.primaryKey)",
                        "(n:Transgene) on (n.primaryKey)",
                        "(n:DiseaseEntityJoin) on (n.primaryKey)",
                        "(n:ExperimentalCondition) on (n.primaryKey)",
                        "(n:Species) on (n.primaryKey)",
                        "(n:Entity) on (n.primaryKey)",
                        "(n:Exon) on (n.primaryKey)",
                        "(n:Exon) on (n.gff3ID)",
                        "(n:Synonym) on (n.primaryKey)",
                        "(n:Identifier) on (n.primaryKey)",
                        "(n:Association) on (n.primaryKey)",
                        "(n:InteractionGeneJoin) on (n.primaryKey)",
                        "(n:InteractionGeneJoin) on (n.uuid)",
                        "(n:CrossReference) on (n.primaryKey)",
                        "(n:CrossReference) on (n.globalCrossRefId)",
                        "(n:CrossReference) on (n.localId)",
                        "(n:CrossReference) on (n.crossRefType)",
                        "(n:OrthologyGeneJoin) on (n.primaryKey)",
                        "(n:GOTerm) on (n.isObsolete)",
                        "(n:DOTerm) on (n.isObsolete)",
                        "(n:UBERONTerm) on (n.isObsolete)",
                        "(n:Ontology) on (n.isObsolete)",
                        "(n:SecondaryId) on (n.primaryKey)",
                        "(n:Chromosome) on (n.primaryKey)",
                        "(n:OrthoAlgorithm) on (n.name)",
                        "(n:Note) on (n.primaryKey)",
                        "(n:Gene) on (n.modGlobalId)",
                        "(n:Gene) on (n.localId)",
                        "(n:HTPDataset) on (n.primaryKey)",
                        "(n:HTPDatasetSample) on (n.primaryKey)",
                        "(n:CategoryTag) on (n.primaryKey)",
                        "(n:Load) on (n.primaryKey)",
                        "(n:Feature) on (n.primaryKey)",
                        "(n:Allele) on (n.primaryKey)",
                        "(n:MITerm) on (n.primaryKey)",
                        "(n:Phenotype) on (n.primaryKey)",
                        "(n:PhenotypeEntityJoin) on (n.primaryKey)",
                        "(n:ProteinSequence) on (n.primaryKey)",
                        "(n:CDSSequence) on (n.primaryKey)",
                        "(n:ExpressionBioEntity) on (n.primaryKey)",
                        "(n:Stage) on (n.primaryKey)",
                        "(n:PublicationJoin) on (n.primaryKey)",
                        "(n:PhenotypePublicationJoin) on (n.primaryKey)",
                        "(n:Variant) on (n.primaryKey)",
                        "(n:Variant) on (n.hgvsNomenclature)",
                        "(n:VariantProteinSequence) on (n.primaryKey)",
                        "(n:VariantProteinSequence) on (n.transcriptId)",
                        "(n:VariantProteinSequence) on (n.variantId)",
                        "(n:SequenceTargetingReagent) on (n.primaryKey)",
                        "(n:ECOTerm) on (n.primaryKey)",
                        "(n:ZFATerm) on (n.primaryKey)",
                        "(n:ZFSTerm) on (n.primaryKey)",
                        "(n:CLTerm) on (n.primaryKey)",
                        "(n:WBBTTerm) on (n.primaryKey)",
                        "(n:FBCVTerm) on (n.primaryKey)",
                        "(n:FBBTTerm) on (n.primaryKey)",
                        "(n:MATerm) on (n.primaryKey)",
                        "(n:EMAPATerm) on (n.primaryKey)",
                        "(n:UBERONTerm) on (n.primaryKey)",
                        "(n:PATOTerm) on (n.primaryKey)",
                        "(n:APOTerm) on (n.primaryKey)",
                        "(n:DPOTerm) on (n.primaryKey)",
                        "(n:FYPOTerm) on (n.primaryKey)",
                        "(n:WBPhenotypeTerm) on (n.primaryKey)",
                        "(n:MPTerm) on (n.primaryKey)",
                        "(n:HPTerm) on (n.primaryKey)",
                        "(n:OBITerm) on (n.primaryKey)",
                        "(n:BTOTerm) on (n.primaryKey)",
                        "(n:CHEBITerm) on (n.primaryKey)",
                        "(n:ZECOTerm) on (n.primaryKey)",
                        "(n:MMUSDVTerm) on (n.primaryKey)",
                        "(n:BSPOTerm) on (n.primaryKey)",
                        "(n:MMOTerm) on (n.primaryKey)",
                        "(n:WBLSTerm) on (n.primaryKey)",
                        "(n:XPOTerm) on (n.primaryKey)",
                        "(n:BioEntityGeneExpressionJoin) on (n.primaryKey)",
                        "(n:NonBGIConstructComponent) on (n.primaryKey)"]

            for index in indicies:
                session.run("CREATE INDEX FOR " + index)

            two_composite_indices = [["n:Gene", "n.gff3ID", "n.dataProvider"], # transcript_etl
                                    ["n:Transcript", "n.gff3ID", "n.dataProvider"] # transcript_etl"
                                    ]

            for index in two_composite_indices:
                session.run("CREATE INDEX FOR ({}) ON ({}, {})".format(index[0], index[1], index[2]))
