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
                        "(n:BioEntityGeneExpressionJoin) on (n.primaryKey)"]

            for index in indicies:
                session.run("CREATE INDEX FOR " + index)
        graph.close()
