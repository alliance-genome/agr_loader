import logging
logger = logging.getLogger(__name__)
from etl import ETL
from .helpers import Neo4jHelper
from transactors import CSVTransactor
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

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        for thread in thread_pool:
            thread.join()

    def _process_sub_type(self, subtype):

        logger.info("Starting Expression Ribbon Data")
        query_list = [
            [ExpressionRibbonETL.insert_gocc_ribbon_terms, "10000",
             "expression_gocc_ribbon_terms.csv"],
            [ExpressionRibbonETL.insert_gocc_self_ribbon_terms, "10000",
             "expression_gocc_self_ribbon_terms" + ".csv"],
            [ExpressionRibbonETL.insert_ribonless_ebes, "10000",
             "expression_ribbonless_ebes" + ".csv"]
        ]

        generators = [self.get_ribbon_terms(),
                      self.retrieve_gocc_self_ribbon_terms(),
                      self.retrieve_gocc_ribbonless_ebes()
                      ]

        CSVTransactor.save_file_static(generators, query_list)
        logger.info("Finished Expression Ribbon Data")

    def get_ribbon_terms(self):

        logger.info("made it to the gocc ribbon retrieve")

        expression_gocc_ribbon_retrieve = """
                    MATCH (ebe:ExpressionBioEntity)--(go:GOTerm:Ontology)-[:PART_OF|IS_A*]->(slimTerm:GOTerm:Ontology) 
                    where slimTerm.subset =~ '.*goslim_agr.*'
                    return ebe.primaryKey, slimTerm.primaryKey
                    """

        returnSet = Neo4jHelper().run_single_query(expression_gocc_ribbon_retrieve)

        gocc_ribbon_data = []

        for record in returnSet:
            row = dict(ebe_id=record["ebe.primaryKey"],
                       go_id=record["slimTerm.primaryKey"])
            gocc_ribbon_data.append(row)


        yield [gocc_ribbon_data]

    def retrieve_gocc_ribbon_terms(self):

        logger.info("made it to the gocc ribbon retrieve")
        expression_gocc_ribbon_retrieve = """
            MATCH (ebe:ExpressionBioEntity)--(go:GOTerm:Ontology)-[:PART_OF|IS_A*]->(slimTerm:GOTerm:Ontology) 
            where slimTerm.subset =~ '.*goslim_agr.*'
            return ebe.primaryKey, slimTerm.primaryKey
            """

        returnSet = Neo4jHelper().run_single_query(expression_gocc_ribbon_retrieve)

        gocc_ribbon_data = []

        for record in returnSet:
            row = dict(ebe_id=record["ebe.primaryKey"],
                       go_id=record["slimTerm.primaryKey"])
            gocc_ribbon_data.append(row)

        yield [gocc_ribbon_data]

    def retrieve_gocc_self_ribbon_terms(self):
        gocc_self_ribbon_data = []

        gocc_self_ribbon_ebes = """
            MATCH (ebe:ExpressionBioEntity)-[:CELLULAR_COMPONENT]-(got:GOTerm) 
            where got.subset =~ '.*goslim_agr.*'
            return ebe.primaryKey, got.primaryKey; 
        """

        returnSet = Neo4jHelper().run_single_query(gocc_self_ribbon_ebes)
        for record in returnSet:
            row = dict(ebe_id=record["ebe.primaryKey"],
                       go_id=record["got.primaryKey"])

            gocc_self_ribbon_data.append(row)

        yield [gocc_self_ribbon_data]

    def retrieve_gocc_ribbonless_ebes(self):
        ribbonless_ebes = """
            MATCH (ebe:ExpressionBioEntity)-[:CELLULAR_COMPONENT]-(got:GOTerm) 
            WHERE not ((ebe)-[:CELLULAR_COMPONENT_RIBBON_TERM]->(:GOTerm)) RETURN ebe.primaryKey;           
        """
        returnSet = Neo4jHelper().run_single_query(ribbonless_ebes)

        gocc_ribbonless_data = []

        for record in returnSet:
            row = dict(ebe_id=record["ebe.primaryKey"])
            gocc_ribbonless_data.append(row)

        yield [gocc_ribbonless_data]

