"""Neo4j Helper"""

import logging

from neo4j import GraphDatabase
from loader_common import ContextInfo


class Neo4jHelper():
    """Neo4j Helper"""

    logger = logging.getLogger(__name__)
    context_info = ContextInfo()

    @staticmethod
    def run_single_parameter_query(query, parameter):
        """Run single parameter query"""

        uri = "bolt://" + Neo4jHelper.context_info.env["NEO4J_HOST"] \
                + ":" + str(Neo4jHelper.context_info.env["NEO4J_PORT"])
        graph = GraphDatabase.driver(uri,
                                     auth=("neo4j", "neo4j"),
                                     max_connection_pool_size=-1)

        Neo4jHelper.logger.debug("Running run_single_parameter_query. Please wait...")
        Neo4jHelper.logger.debug("Query: %s", query)
        with graph.session() as session:
            with session.begin_transaction() as transaction:
                return_set = transaction.run(query, parameter=parameter)
        return return_set

    @staticmethod
    def run_single_query(query):
        """Run Single Query"""

        uri = "bolt://" + Neo4jHelper.context_info.env["NEO4J_HOST"] \
              + ":" + str(Neo4jHelper.context_info.env["NEO4J_PORT"])
        graph = GraphDatabase.driver(uri,
                                     auth=("neo4j", "neo4j"),
                                     max_connection_pool_size=-1)

        with graph.session() as session:
            with session.begin_transaction() as transaction:
                return_set = transaction.run(query)
        return return_set

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
        """Create Indicies"""

        uri = "bolt://" + Neo4jHelper.context_info.env["NEO4J_HOST"] \
                + ":" + str(Neo4jHelper.context_info.env["NEO4J_PORT"])
        driver = GraphDatabase.driver(uri,
                                      auth=("neo4j", "neo4j"),
                                      max_connection_pool_size=-1)

        with driver.session() as session:
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
                        ":MMUSDVTerm(primaryKey)",
                        ":BSPOTerm(primaryKey)",
                        ":MMOTerm(primaryKey)",
                        ":WBLSTerm(primaryKey)",
                        ":BioEntityGeneExpressionJoin(primaryKey)",
                        ":Stage(primaryKey)"]

            for index in indicies:
                session.run("CREATE INDEX ON " + index)
