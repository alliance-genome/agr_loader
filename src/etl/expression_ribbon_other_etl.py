import logging
logger = logging.getLogger(__name__)
from etl import ETL
from .helpers import Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor

class ExpressionRibbonOtherETL(ETL):

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config


    ribbonless_ebes = """
        MATCH (ebe:ExpressionBioEntity)-[:CELLULAR_COMPONENT]-(got:GOTerm:Ontology) 
        WHERE not ((ebe)-[:CELLULAR_COMPONENT_RIBBON_TERM]->(:GOTerm:Ontology)) RETURN ebe.primaryKey;           
    """

    insert_ribonless_ebes = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_id})
                MATCH (goterm:GOTerm:Ontology {primaryKey:'GO:otherLocations'})
                MERGE (ebe)-[ebegoccother:CELLULAR_COMPONENT_RIBBON_TERM]-(goterm)
    """

    def _load_and_process_data(self):
        logger.info("Starting Expression Ribbon Data")
        query_list = [
            [ExpressionRibbonOtherETL.insert_ribonless_ebes, "30000", "expression_ribbonless_ebes" + ".csv"]
        ]

        generators = self.get_ribbon_terms()

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        logger.info("Finished Expression Ribbon Data")


    def get_ribbon_terms(self):
        logger.debug("made it to the gocc ribbon retrieve")

        returnSetRLE = Neo4jHelper().run_single_query(self.ribbonless_ebes)

        gocc_ribbonless_data = []

        for record in returnSetRLE:
            row = dict(ebe_id=record["ebe.primaryKey"])
            gocc_ribbonless_data.append(row)

        yield [gocc_ribbonless_data]

