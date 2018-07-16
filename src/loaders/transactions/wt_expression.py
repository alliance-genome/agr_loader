from .transaction import Transaction


class WTExpressionTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def wt_expression_object_tx(self, data, species):
        # Loads the Phenotype data into Neo4j.

        WTExpression = """
            UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey: row.geneId})
            MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId})
            MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
            MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
            MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
            MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})

            MERGE (stage:Stage {primaryKey:row.whenExpressedStage}

            MERGE (e:Expression {primaryKey: row.expressionEntityPk})
            SET e.uuid = row.expressionEntityUuid

            MERGE (g)-[ge:

            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.loadName = "WT-Expression"
                SET l.dataProviders = row.dataProviders
                SET l.dataProvider = row.dataProvider

            MERGE (wtea:Association {primaryKey:row.uuid})
                SET wtea :ExpressionEntityJoin
                SET wtea.joinType = 'expression'
                SET wtea.dataProviders = row.dataProviders

            MERGE (g)-[gwtea:ASSOCIATION]->(wtea)

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId
                SET pubf.pubMedId = row.pubMedId
                SET pubf.pubModUrl = row.pubModUrl
                SET pubf.pubMedUrl = row.pubMedUrl

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
            MERGE (wtea)-[wteapubf:EVIDENCE]->(pubf)

            """

        Transaction.execute_transaction(self, WTExpression, data)

