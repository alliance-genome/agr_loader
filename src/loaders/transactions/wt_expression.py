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
            MATCH (assay:Ontology {primaryKey:row.assay})
            
            OPTIONAL MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
            OPTIONAL MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
            OPTIONAL MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId})
                      
            OPTIONAL MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
            OPTIONAL MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
            OPTIONAL MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})
            
            MERGE (stage:Stage {primaryKey:row.whenExpressedStage})
        
            MERGE (e:ExpressionBioEntity {primaryKey:row.expressionEntityPk})
                SET e.whereExpressedStatement = row.whereExpressedStatement
                SET e.uuid = row.expressionEntityUuid

            MERGE (g)-[gex:EXPRESSED_IN]-(e)
               SET gex.uuid = row.uuidGeneExpressionJoin

            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.loadName = "WT-Expression"
                SET l.dataProviders = row.dataProviders
                SET l.dataProvider = row.dataProvider

            MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.uuidGeneExpressionJoin})
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
            
            //where only cc term exists
            
            WITH otcct, otast, otcctq row WHERE NOT otcct IS NULL and otast IS NULL
                MERGE (ccj:CellularComponentBioEntityJoin {primaryKey:row.uuidCCJoin})
                MERGE (gej)-[gejccj:ASSOCIATION]-(ccj)
                    SET gejccj.uuid = row.uuidCCJoin

                MERGE (otcct)-[otcctccj:ASSOCIATION]-(ccj)
                MERGE (gej)-[gejotcct:CELLULAR_COMPONENT]-(otcct)
                    SET gejotcct.uuid = row.uuidCCJoin
                    
                WITH otcct, otcctq, row WHERE NOT otcct IS NULL and NOT otcctq IS NULL
                    MERGE (ccj)-[ccjotcctq:QUALIFIED_BY]-(otcctq)
            
            // where cc term and ao term exist
            
            WITH otcct, otast, otcctq row WHERE NOT otcct IS NULL and NOT otast IS NULL
                MERGE (ccj:CellularComponentBioEntityJoin {primaryKey:row.uuidCCJoin})
                MERGE (gej)-[gejccj:ASSOCIATION]-(ccj)
                    SET gejccj.uuid = row.uuidCCJoin

                MERGE (otcct)-[otcctccj:ASSOCIATION]-(ccj)
                MERGE (gej)-[gejotcct:CELLULAR_COMPONENT]-(otcct)
                    SET gejotcct.uuid = row.uuidCCJoin
                    
                WITH otcct, otcctq, row WHERE NOT otcct IS NULL and NOT otcctq IS NULL
                    MERGE (ccj)-[ccjotcctq:QUALIFIED_BY]-(otcctq)  
                    MERGE (otcct)-[otcctotast:PART_OF]-(otast)
                    SET otcctotast.uuid = row.uuidGeneExpressionJoin
            
                    
            """

        CCExpression = WTExpression + """
        
            WITH g, assay, otcct, otcctq, row WHERE NOT otcct IS NULL
                MERGE (ccj:CellularComponentBioEntityJoin {primaryKey:row.uuidCCJoin})
                MERGE (gej)-[gejccj:ASSOCIATION]-(ccj)
                    SET gejccj.uuid = row.uuidCCJoin

                MERGE (otcct)-[otcctccj:ASSOCIATION]-(ccj)
                MERGE (gej)-[gejotcct:CELLULAR_COMPONENT]-(otcct)
                    SET gejotcct.uuid = row.uuidCCJoin
                
                WITH g, assay, otcct, otcctq, row WHERE NOT otcct IS NULL and NOT otcctq IS NULL
                    MERGE (ccj)-[ccjotcctq:QUALIFIED_BY]-(otcctq)
                    MERGE (otcct)-[otcctotast:PART_OF]-(otast)
                    SET otcctotast.uuid = row.uuidGeneExpressionJoin


        """
        AOCCExpression = WTExpression + CCExpression + """
        
        WITH g, assay, otast, row
            MERGE (gej)-[gejotast:ANATOMICAL_STRUCUTRE]-(otast)
            MERGE (asj:AnatomicalStructureJoin {primaryKey:row.uuidASJoin})
            MERGE (gej)-[gejasj:ASSOCIATION]-(asj)
                SET gejasj.uuid = row.uuidASJoin

            MERGE (otast)-[otastasj:ASSOCIATION]-(asj)
                SET otastasj.uuid = row.uuidASJ
            MERGE (asj)-[asjotastq:QUALIFIED_BY]-(otastq)

            MERGE (gej)-[gejotasst:ANATOMICAL_SUBSTRUCTURE]-(otasst)

            WITH g, assay, otasst, otasst row WHERE NOT otasst is NULL and NOT otasst IS NULL
                MERGE (assj:AnatomicalSubStructureJoin {primaryKey:row.uuidASSJoin})
                MERGE (assj)-[assjotasst:ASSOCIATION]-(otasst)
                    SET assjotasst.uuid = row.uuidASSJoin
        
                MERGE (gej)-[gejassj:ASSOCIATION]-(assj)
                    SET gejassj.uuid = row.uuidASSJ
        
        """




        Transaction.execute_transaction(self, WTExpression, data)
        print ("EXECUTED EXPRESSION TXN")
