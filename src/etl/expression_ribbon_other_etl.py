"""Expression Ribbon Other ETL"""

import logging

from etl import ETL
from transactors import CSVTransactor, Neo4jTransactor
from .helpers import Neo4jHelper


class ExpressionRibbonOtherETL(ETL):
    """Expression Ribbon Other ETL"""

    logger = logging.getLogger(__name__)

    # Querys which do not take params and can be used as is

    ribbonless_ebes_query = """
        MATCH (ebe:ExpressionBioEntity)-[:CELLULAR_COMPONENT]-(got:GOTerm:Ontology) 
        WHERE not ((ebe)-[:CELLULAR_COMPONENT_RIBBON_TERM]->(:GOTerm:Ontology)) RETURN ebe.primaryKey;           
    """

    # Query templates which take params and will be processed later

    insert_ribonless_ebes_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_id})
                MATCH (goterm:GOTerm:Ontology {primaryKey:'GO:otherLocations'})
                MERGE (ebe)-[ebegoccother:CELLULAR_COMPONENT_RIBBON_TERM]-(goterm) """


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config


    def _load_and_process_data(self):
        self.logger.info("Starting Expression Ribbon Data")

        query_template_list = [
            [self.insert_ribonless_ebes_query_template, "30000", "expression_ribbonless_ebes" + ".csv"]
        ]

        generators = self.get_ribbon_terms()

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        self.logger.info("Finished Expression Ribbon Data")


    def get_ribbon_terms(self):
        """Gets Robbon Terms"""

        self.logger.debug("made it to the gocc ribbon retrieve")

        return_set_rle = Neo4jHelper().run_single_query(self.ribbonless_ebes_query)

        gocc_ribbonless_data = []

        for record in return_set_rle:
            row = dict(ebe_id=record["ebe.primaryKey"])
            gocc_ribbonless_data.append(row)

        yield [gocc_ribbonless_data]
