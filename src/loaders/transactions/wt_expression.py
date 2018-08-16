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
                      
            //OPTIONAL MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
            OPTIONAL MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
            //OPTIONAL MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})
            
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
            
            //where only ao term exists
            
            MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                SET pubf.pubModId = row.pubModId
                SET pubf.pubMedId = row.pubMedId
                SET pubf.pubModUrl = row.pubModUrl
                SET pubf.pubMedUrl = row.pubMedUrl

            MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
            MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) 
               
            
         WITH g, assay, otast, otastq, otcct, otasst, row WHERE NOT otast IS NULL and otasst IS NULL and otcct IS NULL and otastq is null
            MERGE (gej)-[gejotast:ANATOMICAL_STRUCUTRE]-(otast)
            MERGE (asj:AnatomicalStructureJoin {primaryKey:row.uuidASJoin})
            MERGE (gej)-[gejasj:ASSOCIATION]-(asj)
                SET gejasj.uuid = row.uuidASJoin

            MERGE (otast)-[otastasj:ASSOCIATION]-(asj)
                SET otastasj.uuid = row.uuidASJ
                
          
          """

        EASSubstructure = """
                UNWIND $data as row
        
                    MATCH (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.uuidGeneExpressionJoin})
                    MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
                
                    MERGE (assj:AnatomicalSubStructureJoin {primaryKey:row.uuidASSJoin})
                    MERGE (assj)-[assjotasst:ASSOCIATION]-(otasst)
                        SET assjotasst.uuid = row.uuidASSJoin
                
                    MERGE (gej)-[gejotasst:ANATOMICAL_SUBSTRUCTURE]-(otasst)
                    MERGE (gej)-[gejassj:ASSOCIATION]-(assj)
                        SET gejassj.uuid = row.uuidASSJ
        
        """
        EASQualified = """
            
            UNWIND $data as row
                MATCH (asj:AnatomicalStructureJoin {primaryKey:row.uuidASJoin}))
                MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
                
                MERGE (asj)-[asjotastq:QUALIFIED_BY]-(otastq)
          
            """

        CCExpression = """
        
            UNWIND $data as row
            MATCH (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.uuidGeneExpressionJoin})
            MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId}) 
            
            MERGE (ccj:CellularComponentBioEntityJoin {primaryKey:row.uuidCCJoin})
            MERGE (gej)-[gejccj:ASSOCIATION]-(ccj)
                SET gejccj.uuid = row.uuidCCJoin

            MERGE (otcct)-[otcctccj:ASSOCIATION]-(ccj)
            MERGE (gej)-[gejotcct:CELLULAR_COMPONENT]-(otcct)
                SET gejotcct.uuid = row.uuidCCJoin


        """

        CCQExpression = """  
        
            UNWIND $data as row
                MATCH (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.uuidGeneExpressionJoin})
                MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId}) 
                MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
                          
                MERGE (ccj)-[ccjotcctq:QUALIFIED_BY]-(otcctq)
                MERGE (otcct)-[otcctotast:PART_OF]-(otast)
                SET otcctotast.uuid = row.uuidGeneExpressionJoin
                    
        """

        Transaction.execute_transaction(self, WTExpression, data)
        Transaction.execute_transaction(self, EASSubstructure, data)
        Transaction.execute_transaction(self, EASQualified, data)
        Transaction.execute_transaction(self, CCExpression, data)
        Transaction.execute_transaction(self, CCQExpression, data)

        print ("EXECUTED EXPRESSION TXN")
