import os, logging

from neo4j import GraphDatabase
from common import ContextInfo

logger = logging.getLogger(__name__)
context_info = ContextInfo()

class Neo4jHelper(object):

    @staticmethod
    def run_single_parameter_query(query, parameter):
        uri = "bolt://" + context_info.env["NEO4J_NQC_HOST"] + ":" + str(context_info.env["NEO4J_NQC_PORT"])
        graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1)
        
        logger.debug("Running run_single_parameter_query. Please wait...")
        logger.debug("Query: %s" % query)
        with graph.session() as session:
            with session.begin_transaction() as tx:
                returnSet = tx.run(query, parameter=parameter)
        return returnSet
    
    @staticmethod
    def run_single_query(query):
        uri = "bolt://" + context_info.env["NEO4J_NQC_HOST"] + ":" + str(context_info.env["NEO4J_NQC_PORT"])
        graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1)
        
        with graph.session() as session:
            with session.begin_transaction() as tx:
                returnSet = tx.run(query)
        return returnSet

    #def execute_transaction_batch(self, query, data, batch_size):
    #    logger.info("Executing batch query. Please wait...")
    #    logger.debug("Query: " + query)
    #    for submission in self.split_into_chunks(data, batch_size):
    #        self.execute_transaction(query, submission)
    #    logger.info("Finished batch loading.")

    #def split_into_chunks(self, data, batch_size):
    #    return (data[pos:pos + batch_size] for pos in range(0, len(data), batch_size))  
    
    @staticmethod
    def create_indices():
        uri = "bolt://" + context_info.env["NEO4J_NQC_HOST"] + ":" + str(context_info.env["NEO4J_NQC_PORT"])
        graph = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"), max_connection_pool_size=-1)
        
        session = graph.session()

        session.run("CREATE INDEX ON :Gene(primaryKey)")
        session.run("CREATE INDEX ON :Gene(taxonId)")
        session.run("CREATE INDEX ON :GOTerm(primaryKey)")
        session.run("CREATE INDEX ON :Genotype(primaryKey)")
        session.run("CREATE INDEX ON :SOTerm(primaryKey)")
        session.run("CREATE INDEX ON :Ontology(primaryKey)")
        session.run("CREATE INDEX ON :DOTerm(primaryKey)")
        session.run("CREATE INDEX ON :DOTerm(oid)")
        session.run("CREATE INDEX ON :GOTerm(oid)")
        session.run("CREATE INDEX ON :Publication(primaryKey)")
        session.run("CREATE INDEX ON :EvidenceCode(primaryKey)")
        session.run("CREATE INDEX ON :Transgene(primaryKey)")
        session.run("CREATE INDEX ON :Fish(primaryKey)")
        session.run("CREATE INDEX ON :DiseaseObject(primaryKey)")
        session.run("CREATE INDEX ON :DiseaseEntityJoin(primaryKey)")
        session.run("CREATE INDEX ON :EnvironmentCondition(primaryKey)")
        session.run("CREATE INDEX ON :Environment(primaryKey)")
        session.run("CREATE INDEX ON :Species(primaryKey)")
        session.run("CREATE INDEX ON :Annotation(primaryKey)")
        session.run("CREATE INDEX ON :Entity(primaryKey)")
        session.run("CREATE INDEX ON :Synonym(primaryKey)")
        session.run("CREATE INDEX ON :Identifier(primaryKey)")
        session.run("CREATE INDEX ON :Association(primaryKey)")
        session.run("CREATE INDEX ON :InteractionGeneJoin(primaryKey)")
        session.run("CREATE INDEX ON :InteractionGeneJoin(uuid)")
        session.run("CREATE INDEX ON :CrossReference(primaryKey)")
        session.run("CREATE INDEX ON :CrossReference(globalCrossRefId)")
        session.run("CREATE INDEX ON :CrossReference(localId)")
        session.run("CREATE INDEX ON :CrossReference(crossRefType)")
        session.run("CREATE INDEX on :CrossReferenceMetaData(primaryKey)")
        session.run("CREATE INDEX ON :OrthologyGeneJoin(primaryKey)")
        session.run("CREATE INDEX ON :GOGeneJoin(primaryKey)")
        session.run("CREATE INDEX ON :SecondaryId(primaryKey)")
        session.run("CREATE INDEX ON :Chromosome(primaryKey)")
        session.run("CREATE INDEX ON :OrthoAlgorithm (name)")
        session.run("CREATE INDEX ON :Gene(modGlobalId)")
        session.run("CREATE INDEX ON :Gene(localId)")
        session.run("CREATE INDEX ON :Load(primaryKey)")
        session.run("CREATE INDEX ON :Feature(primaryKey)")
        session.run("CREATE INDEX ON :Allele(primaryKey)")
        session.run("CREATE INDEX ON :MITerm(primaryKey)")
        session.run("CREATE INDEX ON :Phenotype(primaryKey)")
        session.run("CREATE INDEX ON :ExpressionBioEntity(primaryKey)")
        session.run("CREATE INDEX ON :Assay(primaryKey)")
        session.run("CREATE INDEX ON :Stage(primaryKey)")
        session.run("CREATE INDEX ON :PublicationEvidenceCodeJoin(primaryKey)")
        session.run("CREATE INDEX ON :Variant(primaryKey)")
        
        session.run("CREATE INDEX ON :ZFATerm(primaryKey)")
        session.run("CREATE INDEX ON :ZFSTerm(primaryKey)")
        session.run("CREATE INDEX ON :CLTerm(primaryKey)")
        session.run("CREATE INDEX ON :WBBTTerm(primaryKey)")
        session.run("CREATE INDEX ON :FBCVTerm(primaryKey)")
        session.run("CREATE INDEX ON :MATerm(primaryKey)")
        session.run("CREATE INDEX ON :EMAPATerm(primaryKey)")
        session.run("CREATE INDEX ON :UBERONTerm(primaryKey)")
        session.run("CREATE INDEX ON :FBCVTerm(primaryKey)")
        session.run("CREATE INDEX ON :MMUSDVTerm(primaryKey)")
        session.run("CREATE INDEX ON :BSPOTerm(primaryKey)")
        session.run("CREATE INDEX ON :MMOTerm(primaryKey)")
        session.run("CREATE INDEX ON :WBLSTerm(primaryKey)")
        session.run("CREATE INDEX ON :BioEntityGeneExpressionJoin(primaryKey)")
        session.run("CREATE INDEX ON :Stage(primaryKey)")

        session.close()
