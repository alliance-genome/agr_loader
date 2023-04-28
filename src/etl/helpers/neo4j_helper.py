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

            # A list of indices to create. 
            # IMPORTANT: If an entry already exists in the constraint list, it also receives an index and does not need to be added here.
            indicies = ["(n:BioEntityGeneExpressionJoin) on (n.primaryKey)",
                        "(n:CDS) on (n.gff3ID)",
                        "(n:CDSSequence) on (n.primaryKey)",
                        "(n:CrossReference) on (n.crossRefType)",
                        "(n:CrossReference) on (n.globalCrossRefId)",
                        "(n:CrossReference) on (n.localId)",
                        "(n:DOTerm) on (n.isObsolete)",
                        "(n:DOTerm) on (n.oid)",
                        "(n:ExpressionBioEntity) on (n.primaryKey)",
                        "(n:Feature) on (n.primaryKey)",
                        "(n:Gene) on (n.localId)",
                        "(n:Gene) on (n.modGlobalId)",
                        "(n:Gene) on (n.modLocalId)",
                        "(n:Gene) on (n.symbol)",
                        "(n:Gene) on (n.taxonId)",
                        "(n:Gene) on (n.gff3ID)",
                        "(n:GeneLevelConsequence) on (n.primaryKey)",
                        "(n:GOTerm) on (n.isObsolete)",
                        "(n:GOTerm) on (n.oid)",
                        "(n:InteractionGeneJoin) on (n.uuid)",
                        "(n:Load) on (n.primaryKey)",
                        "(n:Ontology) on (n.isObsolete)",
                        "(n:Phenotype) on (n.primaryKey)",
                        "(n:PhenotypePublicationJoin) on (n.primaryKey)",
                        "(n:ProteinSequence) on (n.primaryKey)",
                        "(n:PublicationJoin) on (n.primaryKey)",
                        "(n:SOTerm) on (n.name)",
                        "(n:Stage) on (n.primaryKey)",
                        "(n:Synonym) on (n.primaryKey)",
                        "(n:Transcript) on (n.dataProvider)",
                        "(n:Transcript) on (n.gff3ID)",
                        "(n:TranscriptLevelConsequence) on (n.primaryKey)",
                        "(n:TranscriptProteinSequence) on (n.primaryKey)",
                        "(n:UBERONTerm) on (n.isObsolete)",
                        "(n:VariantProteinSequence) on (n.transcriptId)",
                        "(n:VariantProteinSequence) on (n.variantId)"]

            for index in indicies:
                session.run("CREATE INDEX FOR " + index)

            two_composite_indices = [["n:Gene", "n.gff3ID", "n.dataProvider"], # transcript_etl
                                    ["n:Transcript", "n.gff3ID", "n.dataProvider"] # transcript_etl"
                                    ]

            for index in two_composite_indices:
                session.run("CREATE INDEX FOR ({}) ON ({}, {})".format(index[0], index[1], index[2]))
