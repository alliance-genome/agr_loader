from .transaction import Transaction
from services import CreateCrossReference


class WTExpressionTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def wt_expression_object_tx(self, data, species):
        # Loads the Phenotype data into Neo4j.

        WTExpression = """
            UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            
            MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
            
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
            MATCH (assay:Ontology {primaryKey:row.assay})
    
            MERGE (stage:Stage {primaryKey:row.whenExpressedStage})
            
            MERGE (e:ExpressionBioEntity {primaryKey:row.expressionEntityPk})
                SET e.whereExpressedStatement = row.whereExpressedStatement

            MERGE (g)-[gex:EXPRESSED_IN]-(e)
               SET gex.uuid = row.uuidGeneExpressionJoin


        """
        print ("EXECUTED EXPRESSION TXN")
        Transaction.execute_transaction(self, WTExpression, data)
        print ("EXECUTED EXPRESSION TXN")

