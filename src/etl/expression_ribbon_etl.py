import logging
logger = logging.getLogger(__name__)
from etl import ETL
from .helpers import Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor
import multiprocessing

class ExpressionRibbonETL(ETL):

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    insert_gocc_self_ribbon_terms = """
                USING PERIODIC COMMIT %s
                LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                   MATCH (ebe:ExpressionBioEntity) where ebe.primaryKey = row.ebe_id
                   MATCH (goTerm:GOTerm:Ontology) where goTerm.primaryKey = row.go_id

                   MERGE (ebe)-[ebego:CELLULAR_COMPONENT_RIBBON_TERM]-(goTerm)
                   """

    insert_gocc_ribbon_terms = """
                USING PERIODIC COMMIT %s
                LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                   MATCH (ebe:ExpressionBioEntity) where ebe.primaryKey = row.ebe_id
                   MATCH (goTerm:GOTerm:Ontology) where goTerm.primaryKey = row.go_id

                   MERGE (ebe)-[ebego:CELLULAR_COMPONENT_RIBBON_TERM]-(goTerm)
                   """

    insert_ribonless_ebes = """
                USING PERIODIC COMMIT %s
                LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                    MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_id})
                    MATCH (goterm:GOTerm:Ontology {primaryKey:'GO:otherLocations'})
                    MERGE (ebe)-[ebegoccother:CELLULAR_COMPONENT_RIBBON_TERM]-(goterm)
        """

    expression_gocc_ribbon_retrieve = """
                MATCH (ebe:ExpressionBioEntity)--(go:GOTerm:Ontology)-[:PART_OF|IS_A*]->(slimTerm:GOTerm:Ontology) 
                where slimTerm.subset =~ '.*goslim_agr.*'
                return ebe.primaryKey, slimTerm.primaryKey
                """

    gocc_self_ribbon_ebes = """
        MATCH (ebe:ExpressionBioEntity)-[:CELLULAR_COMPONENT]-(got:GOTerm:Ontology) 
        where got.subset =~ '.*goslim_agr.*'
        return ebe.primaryKey, got.primaryKey; 
        """

    ribbonless_ebes = """
        MATCH (ebe:ExpressionBioEntity)-[:CELLULAR_COMPONENT]-(got:GOTerm:Ontology) 
        WHERE not ((ebe)-[:CELLULAR_COMPONENT_RIBBON_TERM]->(:GOTerm:Ontology)) RETURN ebe.primaryKey;           
    """

    uberonAO = """  
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid})  
            MATCH (o:UBERONTerm {primaryKey:row.aoUberonId})     
            MERGE (ebe)-[ebeo:ANATOMICAL_RIBBON_TERM]-(o) """

    uberonStage = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})  
            MATCH (o:UBERONTerm {primaryKey:row.uberonStageId})

            MERGE (ei)-[eio:STAGE_RIBBON_TERM]-(o) """

    uberonAOOther = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid}) 
            MATCH (u:UBERONTerm:Ontology {primaryKey:'UBERON:AnatomyOtherLocation'}) 
            MERGE (ebe)-[ebeu:ANATOMICAL_RIBBON_TERM]-(u) """

    uberonStageOther = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
            MATCH (u:UBERONTerm:Ontology {primaryKey:'UBERON:PostEmbryonicPreAdult'})

            MERGE (ei)-[eiu:STAGE_RIBBON_TERM]-(u) """

    def _load_and_process_data(self):

        logger.info("Starting Expression Ribbon Data")
        query_list = [
            [ExpressionRibbonETL.insert_gocc_ribbon_terms, "10000", "expression_gocc_ribbon_terms.csv"] ,
            [ExpressionRibbonETL.insert_gocc_self_ribbon_terms, "10000", "expression_gocc_self_ribbon_terms" + ".csv"],
            [ExpressionRibbonETL.insert_ribonless_ebes, "10000", "expression_ribbonless_ebes" + ".csv"]
        ]

        generators = self.get_ribbon_terms()


        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        
        logger.info("Finished Expression Ribbon Data")

    def get_ribbon_terms(self):

        logger.debug("made it to the gocc ribbon retrieve")


        returnSetRT = Neo4jHelper().run_single_query(self.expression_gocc_ribbon_retrieve)

        gocc_ribbon_data = []

        for record in returnSetRT:
            row = dict(ebe_id=record["ebe.primaryKey"],
                       go_id=record["slimTerm.primaryKey"])
            gocc_ribbon_data.append(row)

        gocc_self_ribbon_data = []

        returnSetSRT = Neo4jHelper().run_single_query(self.gocc_self_ribbon_ebes)

        for record in returnSetSRT:
            row = dict(ebe_id=record["ebe.primaryKey"],
                       go_id=record["got.primaryKey"])

            gocc_self_ribbon_data.append(row)

        returnSetRLE = Neo4jHelper().run_single_query(self.ribbonless_ebes)

        gocc_ribbonless_data = []

        for record in returnSetRLE:
            row = dict(ebe_id=record["ebe.primaryKey"])
            gocc_ribbonless_data.append(row)

        yield [gocc_ribbon_data, gocc_self_ribbon_data, gocc_ribbonless_data]

