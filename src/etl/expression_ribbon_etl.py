import logging
logger = logging.getLogger(__name__)
from etl import ETL
from helpers import Neo4jHelper
from transactors import CSVTransactor

class ExpressionRibbonETL(ETL):

    def __init__(self):
        super().__init__()


    def _load_and_process_data(self):

        gocc_ribbon_data = self.retrieve_gocc_ribbon_terms()
        gocc_self_ribbon_data = self.retrieve_gocc_self_ribbon_terms()
        gocc_ribbonless_data = self.retrieve_gocc_ribbonless_ebes()

        self.insert_gocc_ribbon_terms(gocc_ribbon_data)
        self.insert_gocc_self_ribbon_terms(gocc_self_ribbon_data)
        self.insert_ribonless_ebes(gocc_ribbonless_data)


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

    def insert_gocc_self_ribbon_terms(self, gocc_self_ribbon_data):
        expression_gocc_self_ribbon_insert = """
                   UNWIND $data as row

                   MATCH (ebe:ExpressionBioEntity) where ebe.primaryKey = row.ebe_id
                   MATCH (goTerm:GOTerm:Ontology) where goTerm.primaryKey = row.go_id

                   MERGE (ebe)-[ebego:CELLULAR_COMPONENT_RIBBON_TERM]-(goTerm)
                   """

        self.execute_transaction(expression_gocc_self_ribbon_insert, gocc_self_ribbon_data)

    def insert_gocc_ribbon_terms(self, gocc_ribbon_data):
        expression_gocc_ribbon_insert = """
                   UNWIND $data as row

                   MATCH (ebe:ExpressionBioEntity) where ebe.primaryKey = row.ebe_id
                   MATCH (goTerm:GOTerm:Ontology) where goTerm.primaryKey = row.go_id

                   MERGE (ebe)-[ebego:CELLULAR_COMPONENT_RIBBON_TERM]-(goTerm)
                   """

        self.execute_transaction(expression_gocc_ribbon_insert, gocc_ribbon_data)

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

    def insert_ribonless_ebes(self, gocc_ribbonless_data):
        insert_ribbonless_data = """
                UNWIND $data as row
                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_id})
                MATCH (goterm:GOTerm:Ontology {primaryKey:'GO:otherLocations'})
                MERGE (ebe)-[ebegoccother:CELLULAR_COMPONENT_RIBBON_TERM]-(goterm)
        """

        self.execute_transaction(insert_ribbonless_data, gocc_ribbonless_data)