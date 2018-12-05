import logging
logger = logging.getLogger(__name__)
from etl import ETL
from helpers import Neo4jHelper
from transactors import CSVTransactor

class ExpressionRibbonETL(ETL):

    def __init__(self):
        super().__init__()

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

        logger.info("Finished Retrieving Expression Ribbon Data:")

        commit_size = self.data_type_config.get_neo4j_commit_size()

        query_list = [
            [ExpressionRibbonETL.insert_gocc_ribbon_terms, commit_size,
             "expression_gocc_ribbon_terms" + ".csv"],
            [ExpressionRibbonETL.insert_gocc_self_ribbon_terms, commit_size,
             "expression_gocc_self_ribbon_terms" + ".csv"],
            [ExpressionRibbonETL.insert_ribonless_ebes, commit_size,
             "expression_ribbonless_ebes" + ".csv"]
        ]
        # Obtain the generator
        generators = self.get_generators()

        # Prepare the transaction
        CSVTransactor.execute_transaction(generators, query_list)

    def get_generators(self):

        gocc_ribbon_data = self.retrieve_gocc_ribbon_terms()
        gocc_self_ribbon_data = self.retrieve_gocc_self_ribbon_terms()
        gocc_ribbonless_data = self.retrieve_gocc_ribbonless_ebes()

        yield [gocc_ribbon_data, gocc_self_ribbon_data, gocc_ribbonless_data]

    def retrieve_gocc_ribbon_terms(self):
        expression_gocc_ribbon_retrieve = """
            MATCH (ebe:ExpressionBioEntity)-->(go:GOTerm:Ontology)-[:PART_OF|IS_A*]->(slimTerm:GOTerm:Ontology) 
            where all (subset IN ['goslim_agr'] where subset in slimTerm.subset)
            return ebe.primaryKey, slimTerm.primaryKey
            """

        returnSet = Neo4jHelper().run_single_query(expression_gocc_ribbon_retrieve)

        gocc_ribbon_data = []

        for record in returnSet:
            row = dict(ebe_id=record["ebe.primaryKey"],
                       go_id=record["slimTerm.primaryKey"])
            gocc_ribbon_data.append(row)

        return gocc_ribbon_data

    def retrieve_gocc_self_ribbon_terms(self):
        gocc_self_ribbon_data = []

        gocc_self_ribbon_ebes = """
            MATCH (ebe:ExpressionBioEntity)-[:CELLULAR_COMPONENT]-(got:GOTerm) 
            where 'goslim_agr' in got.subset
            return ebe.primaryKey, got.primaryKey; 
        """

        returnSet = Neo4jHelper().run_single_query(gocc_self_ribbon_ebes)
        for record in returnSet:
            row = dict(ebe_id=record["ebe.primaryKey"],
                       go_id=record["got.primaryKey"])

            gocc_self_ribbon_data.append(row)

        return gocc_self_ribbon_data

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

        return gocc_ribbonless_data

