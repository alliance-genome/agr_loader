from .transaction import Transaction
from services import CreateCrossReference


class WTExpressionTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def wt_expression_object_tx(self, data, species):
        # Loads the Phenotype data into Neo4j.

        AOExpression = """
            UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})

            MATCH (assay:Ontology {primaryKey:row.assay})
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
            OPTIONAL MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId})
            
            WITH g, assay, otast, otcct, row WHERE otcct IS NULL

                MERGE (e:ExpressionBioEntity {primaryKey:row.whereExpressedStatement})
                    SET e.whereExpressedStatement = row.whereExpressedStatement
                    
                MERGE (g)-[gex:EXPRESSED_IN]-(e)
                    SET gex.uuid = row.ebe_uuid
            
                MERGE (stage:Stage {primaryKey:row.whenExpressedStage})
                
                MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ebe_uuid})
                    SET gej.joinType = 'expression'
                    SET gej.dataProviders = row.dataProviders
                
                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCATION]->(gej)
                    
                MERGE (gej)-[gejs:DURING]-(stage)
                
                MERGE (gej)-[geja:ASSAY]-(assay)
        
                MERGE (e)-[gejotast:ANATOMICAL_STRUCUTRE]-(otast)
                    SET gejotast.uuid = row.s_uuid
                
                MERGE (asj:AnatomicalStructureExpressionBioEntityJoin:Association {primaryKey:row.s_uuid})
                    SET asj.joinType = 'expression'
                
                MERGE (e)-[gejasj:ASSOCIATION]-(asj)
                MERGE (asj)<-[asjotast:ASSOCIATION]-(otast)
                
                MERGE (l:Load:Entity {primaryKey:row.loadKey})
                    SET l.dateProduced = row.dateProduced
                    SET l.loadName = "WT-Expression"
                    SET l.dataProviders = row.dataProviders
                    SET l.dataProvider = row.dataProvider
            
                //where only ao term exists
            
                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    SET pubf.pubModId = row.pubModId
                    SET pubf.pubMedId = row.pubMedId
                    SET pubf.pubModUrl = row.pubModUrl
                    SET pubf.pubMedUrl = row.pubMedUrl

                MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) 
                
    
        """

        CCExpression = """

        UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            MATCH (assay:Ontology {primaryKey:row.assay})
            MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId})
            OPTIONAL MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId}) 

            WITH g, assay, otcct, otast, row WHERE otast IS NULL 
    
                MERGE (e:ExpressionBioEntity {primaryKey:row.whereExpressedStatement})
                    SET e.whereExpressedStatement = row.whereExpressedStatement
                    
                MERGE (g)-[gex:EXPRESSED_IN]-(e)
                    SET gex.uuid = row.ebe_uuid
                             
                MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ebe_uuid})
                    SET gej.joinType = 'expression'
                    SET gej.dataProviders = row.dataProviders
                
                MERGE (stage:Stage {primaryKey:row.whenExpressedStage})
                
                MERGE (gej)-[gejs:DURING]-(stage)
                MERGE (gej)-[geja:ASSAY]-(assay)

                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCIATION]->(gej)
                
                MERGE (cej:CellularComponentExpressionBioEntityJoin:Association {primaryKey:row.cc_uuid})
                    SET cej.joinType = 'expression'
               
                MERGE (e)-[ecej:ASSOCIATION]->(cej)
                    
                MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)
                    SET eotcct.uuid = row.cc_uuid
                    
                MERGE (otcct)-[otcctcej:ASSOCIATION]->(cej)
                
                MERGE (l:Load:Entity {primaryKey:row.loadKey})
                    SET l.dateProduced = row.dateProduced
                    SET l.loadName = "WT-Expression"
                    SET l.dataProviders = row.dataProviders
                    SET l.dataProvider = row.dataProvider
                    
                //where only ao term exists

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    SET pubf.pubModId = row.pubModId
                    SET pubf.pubMedId = row.pubMedId
                    SET pubf.pubModUrl = row.pubModUrl
                    SET pubf.pubMedUrl = row.pubMedUrl

                MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) 

        """

        AOCCExpression = """


        UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            MATCH (assay:Ontology {primaryKey:row.assay})
            MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId})
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId}) 

            WITH g, assay, otcct, otast, row WHERE NOT otast IS NULL AND NOT otcct IS NULL
                
   
                MERGE (e:ExpressionBioEntity {primaryKey:row.whereExpressedStatement})
                    SET e.whereExpressedStatement = row.whereExpressedStatement
                
                MERGE (g)-[gex:EXPRESSED_IN]-(e)
                    SET gex.uuid = row.ebe_uuid
                             
                MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ebe_uuid})
                    SET gej.joinType = 'expression'
                    SET gej.dataProviders = row.dataProviders
                
                MERGE (stage:Stage {primaryKey:row.whenExpressedStage})
                
                MERGE (gej)-[gejs:DURING]-(stage)
                MERGE (gej)-[geja:ASSAY]-(assay)

                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCIATION]->(gej)
                
                MERGE (cej:CellularComponentExpressionBioEntityJoin:Association {primaryKey:row.cc_uuid})
                    SET cej.joinType = 'expression'
                
                MERGE (e)-[ecej:ASSOCIATION]->(cej)
                
                MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)
                    SET eotcct.uuid = row.cc_uuid
                    
                MERGE (otcct)-[otcctcej:ASSOCIATION]->(cej)
                    
                MERGE (e)-[gejotast:ANATOMICAL_STRUCUTRE]-(otast)
                    SET gejotast.uuid = row.s_uuid
                    
                MERGE (asj:AnatomicalStructureExpressionBioEntityJoin:Association {primaryKey:row.s_uuid})
                SET asj.joinType = 'expression'
                
                MERGE (e)-[gejasj:ASSOCIATION]-(asj)

                MERGE (asj)<-[eotast:ASSOCIATION]-(otast)

                MERGE (l:Load:Entity {primaryKey:row.loadKey})
                    SET l.dateProduced = row.dateProduced
                    SET l.loadName = "WT-Expression"
                    SET l.dataProviders = row.dataProviders
                    SET l.dataProvider = row.dataProvider
                    
                //where only ao term exists

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    SET pubf.pubModId = row.pubModId
                    SET pubf.pubMedId = row.pubMedId
                    SET pubf.pubModUrl = row.pubModUrl
                    SET pubf.pubMedUrl = row.pubMedUrl

                MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) 


        """

        EASSubstructure = """
                UNWIND $data as row
        
                    MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
                    MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
                    MATCH (asj:AnatomicalStructureExpressionBioEntityJoin:Association {primaryKey:row.s_uuid})
                
                    MERGE (assj:AnatomicalSubStructureExpressionBioEntityJoin:Association {primaryKey:row.ss_uuid})
                         SET assj.joinType = 'expression'
                         
                    MERGE (assj)-[assjotasst:ASSOCIATION]->(otasst)
                
                    MERGE (asj)-[asjotasst:ANATOMICAL_SUBSTRUCTURE]->(otasst)
                        SET asjotasst.uuid = row.ss_uuid
                        
                    MERGE (asj)-[asjassj:ASSOCIATION]->(assj)
                        
        """
        EASQualified = """
            
            UNWIND $data as row
                MATCH (asj:AnatomicalStructureExpressionBioEntityJoin:Association {primaryKey:row.s_uuid})
                MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
                
                MERGE (asj)-[asjotastq:QUALIFIED_BY]-(otastq)
          
            """
        EASSQualified = """

            UNWIND $data as row
                MATCH (assj:AnatomicalSubStructureExpressionBioEntityJoin:Association {primaryKey:row.ss_uuid})
                MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})

                MERGE (assj)-[asjotasstq:QUALIFIED_BY]-(otasstq)

            """
        CCQExpression = """  
        
            UNWIND $data as row
                MATCH (ccj:CellularComponentExpressionBioEntityJoin:Association {primaryKey:row.cc_uuid})
                MATCH (otcct:Ontology {primaryKey:row.cellularComponentTermId}) 
                MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
                          
                MERGE (ccj)-[ccjotcctq:QUALIFIED_BY]-(otcctq)
                    
        """

        Transaction.execute_transaction(self, AOExpression, data)
        Transaction.execute_transaction(self, CCExpression, data)
        Transaction.execute_transaction(self, AOCCExpression, data)
        Transaction.execute_transaction(self, EASSubstructure, data)
        Transaction.execute_transaction(self, EASQualified, data)
        Transaction.execute_transaction(self, EASSQualified, data)
        Transaction.execute_transaction(self, CCQExpression, data)

        print ("EXECUTED EXPRESSION TXN")
