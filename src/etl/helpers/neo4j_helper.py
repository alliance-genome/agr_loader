"""Neo4j Helper"""

import logging
from contextlib import contextmanager

from neo4j import GraphDatabase
from loader_common import ContextInfo


logger = logging.getLogger(__name__)
context_info = ContextInfo()
uri = "bolt://" + context_info.env["NEO4J_HOST"] + ":" + str(context_info.env["NEO4J_PORT"])
graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1)


class Neo4jHelper:
    """Neo4j Helper"""

    @staticmethod
    @contextmanager
    def run_single_parameter_query(query, parameter):
        """Run single parameter query"""
        try:
            logger.debug("Running run_single_parameter_query. Please wait...")
            logger.debug("Query: %s", query)
            with graph.session() as session:
                with session.begin_transaction() as transaction:
                    yield transaction.run(query, parameter=parameter)
        finally:
            logger.debug("closing neo4j transaction and session")

    @staticmethod
    @contextmanager
    def run_single_query(query):
        """Run Single Query"""
        try:
            with graph.session() as session:
                with session.begin_transaction() as transaction:
                    yield transaction.run(query)
        finally:
            logger.debug("closing neo4j transaction and session")

    @staticmethod
    def run_single_query_no_return(query):
        """Run Single Query"""
        with graph.session() as session:
            with session.begin_transaction() as transaction:
                transaction.run(query)

    @staticmethod
    def create_indices():
        """Create Indicies"""

        with graph.session() as session:
            indicies = [":CDS(primaryKey)",
                        ":Gene(primaryKey)",
                        ":Gene(modLocalId)",
                        ":Gene(symbol)",
                        ":Gene(gff3ID)",
                        ":Gene(taxonId)",
                        ":Construct(primaryKey)",
                        ":Transcript(primaryKey)",
                        ":Transcript(dataProvider)",
                        ":TranscriptLevelConsequence(primaryKey)",
                        ":TranscriptProteinSequence(primaryKey)",
                        ":GeneLevelConsequence(primaryKey)",
                        ":Transcript(gff3ID)",
                        ":CDS(gff3ID)",
                        ":GOTerm(primaryKey)",
                        ":Genotype(primaryKey)",
                        ":AffectedGenomicModel(primaryKey)",
                        ":SOTerm(primaryKey)",
                        ":SOTerm(name)",
                        ":Ontology(primaryKey)",
                        ":Ontology(name)",
                        ":DOTerm(primaryKey)",
                        ":DOTerm(oid)",
                        ":GOTerm(oid)",
                        ":GenomicLocation(primaryKey)",
                        ":Assembly(primaryKey)",
                        ":Publication(primaryKey)",
                        ":Transgene(primaryKey)",
                        ":DiseaseEntityJoin(primaryKey)",
                        ":ExperimentalCondition(primaryKey)",
                        ":Species(primaryKey)",
                        ":Entity(primaryKey)",
                        ":Exon(primaryKey)",
                        ":Exon(gff3ID)",
                        ":Synonym(primaryKey)",
                        ":Identifier(primaryKey)",
                        ":Association(primaryKey)",
                        ":InteractionGeneJoin(primaryKey)",
                        ":InteractionGeneJoin(uuid)",
                        ":CrossReference(primaryKey)",
                        ":CrossReference(globalCrossRefId)",
                        ":CrossReference(localId)",
                        ":CrossReference(crossRefType)",
                        ":OrthologyGeneJoin(primaryKey)",
                        ":GOTerm(isObsolete)",
                        ":DOTerm(isObsolete)",
                        ":UBERONTerm(isObsolete)",
                        ":Ontology(isObsolete)",
                        ":SecondaryId(primaryKey)",
                        ":Chromosome(primaryKey)",
                        ":OrthoAlgorithm(name)",
                        ":Note(primaryKey)",
                        ":Gene(modGlobalId)",
                        ":Gene(localId)",
                        ":HTPDataset(primaryKey)",
                        ":HTPDatasetSample(primaryKey)",
                        ":CategoryTag(primaryKey)",
                        ":Load(primaryKey)",
                        ":Feature(primaryKey)",
                        ":Allele(primaryKey)",
                        ":MITerm(primaryKey)",
                        ":Phenotype(primaryKey)",
                        ":PhenotypeEntityJoin(primaryKey)",
                        ":ProteinSequence(primaryKey)",
                        ":CDSSequence(primaryKey)",
                        ":ExpressionBioEntity(primaryKey)",
                        ":Stage(primaryKey)",
                        ":PublicationJoin(primaryKey)",
                        ":PhenotypePublicationJoin(primaryKey)",
                        ":Variant(primaryKey)",
                        ":Variant(hgvsNomenclature)",
                        ":VariantProteinSequence(primaryKey)",
                        ":VariantProteinSequence(transcriptId)",
                        ":VariantProteinSequence(variantId)",
                        ":SequenceTargetingReagent(primaryKey)",
                        ":ECOTerm(primaryKey)",
                        ":ZFATerm(primaryKey)",
                        ":ZFSTerm(primaryKey)",
                        ":CLTerm(primaryKey)",
                        ":WBBTTerm(primaryKey)",
                        ":FBCVTerm(primaryKey)",
                        ":FBBTTerm(primaryKey)",
                        ":MATerm(primaryKey)",
                        ":EMAPATerm(primaryKey)",
                        ":UBERONTerm(primaryKey)",
                        ":PATOTerm(primaryKey)",
                        ":APOTerm(primaryKey)",
                        ":DPOTerm(primaryKey)",
                        ":FYPOTerm(primaryKey)",
                        ":WBPhenotypeTerm(primaryKey)",
                        ":MPTerm(primaryKey)",
                        ":HPTerm(primaryKey)",
                        ":OBITerm(primaryKey)",
                        ":BTOTerm(primaryKey)",
                        ":CHEBITerm(primaryKey)",
                        ":ZECOTerm(primaryKey)",
                        ":MMUSDVTerm(primaryKey)",
                        ":BSPOTerm(primaryKey)",
                        ":MMOTerm(primaryKey)",
                        ":WBLSTerm(primaryKey)",
                        ":BioEntityGeneExpressionJoin(primaryKey)",
                        ":Stage(primaryKey)"]

            for index in indicies:
                session.run("CREATE INDEX ON " + index)
