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
            
            FOREACH (rel IN CASE when row.cellularComponentTermId is not null THEN [1] ELSE [] END |
                MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId})
                
            )
            // MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
            
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
    
            MERGE (stage:Stage {primaryKey:row.whenExpressedStage})
            
            MERGE (e:ExpressionBioEntity {primaryKey:row.expressionEntityPk})
                SET e.whereExpressedStatement = row.whereExpressedStatement
                
            MATCH (assay:Ontology {primaryKey:row.assay})

            MERGE (g)-[gex:EXPRESSED_IN]-(e)
               SET gex.uuid = row.uuidGeneExpressionJoin


        """
        print ("EXECUTED EXPRESSION TXN")
        Transaction.execute_transaction(self, WTExpression, data)
        print ("EXECUTED EXPRESSION TXN")

