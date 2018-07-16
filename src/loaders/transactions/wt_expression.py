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
            MATCH (g:Gene {primaryKey: row.geneId})
            MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId})
            MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
            MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
            MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
            MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})

            MERGE (stage:Stage {primaryKey:row.whenExpressedStage})
            MERGE (e:ExpressionBioEntity {primaryKey: row.expressionEntityPk})
            MERGE (assay:Assay {primaryKey:row.assay})

            MERGE (g)-[gex:EXPRESSED_IN]-(e)
               SET gex:uuid = row.uuidGeneExpressionJoin

            MERGE (e)-[:HAS_PART]-(otast)
            MERGE (e)-[:HAS_PART]-(otast)
            MERGE (e)-[:HAS_PART]-(otast)
            MERGE (otast)-[:SUB_STRUCTURE]-(otasst)
            MERGE (otast)-[:QUALIFIED_BY]-(otastq)
            MERGE (otast)-[:QUALIFIED_BY]-(otasstq)
            MERGE (otcct)-[:QUALIFIED_BY]-(otcctq)

            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.loadName = "WT-Expression"
                SET l.dataProviders = row.dataProviders
                SET l.dataProvider = row.dataProvider

            MERGE (gej:GeneExpressionJoin {primaryKey: row.uuidGeneExpressionJoin})
                SET gej.joinType = 'expression'
                SET gej.dataProviders = row.dataProviders

            MERGE (g)-[ggej:ASSOCIATION]->(gej)
            MERGE (e)-[egej:ASSOCIATION]->(gej)

            MERGE (gej)-[gejs:EXPRESSED_DURING]-(stage)
            MERGE (gej)-[geja:ASSAY]-(assay)

            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId
                SET pubf.pubMedId = row.pubMedId
                SET pubf.pubModUrl = row.pubModUrl
                SET pubf.pubMedUrl = row.pubMedUrl

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
            MERGE (gej)-[gejpubf:EVIDENCE]->(pubf)

            WITH o, row.crossReferences AS events
            UNWIND events AS event

        """ + CreateCrossReference.get_cypher_xref_text("expression")


        Transaction.execute_transaction(self, WTExpression, data)

