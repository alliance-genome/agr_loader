from .transaction import Transaction
from services import CreateCrossReference
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class WTExpressionTransaction(Transaction):
    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def wt_expression_object_tx(self, AOExpressionData, CCExpressionData, AOQualifierData, AOSubstructureData,
                                AOSSQualifierData, CCQualifierData, AOCCExpressionData, stageList, stageUberonData,
                                uberonAOData, uberonAOOtherData, uberonStageOtherData, xrefs, species):
        # Loads the Phenotype data into Neo4j.

        xref = """
        UNWIND $data as event
        MATCH (o:BioEntityGeneExpressionJoin:Association {primaryKey:event.ei_uuid})
        
        """ + CreateCrossReference.get_cypher_xref_text("expression")

        AddOther = """
        
            MERGE(other:UBERONTerm:Ontology {primaryKey:'UBERON:AnatomyOtherLocation'})
                ON CREATE SET other.name = 'Other'
            MERGE(otherstage:UBERONTerm:Ontology {primaryKey:'UBERON:PostEmbryonicPreAdult'})
                ON CREATE SET otherstage.name = 'post embryonic, pre-adult'
            MERGE(othergo:GOTerm:Ontology {primaryKey:'GO:otherLocations'})
                ON CREATE SET othergo.name = 'other locations'
                ON CREATE SET othergo.definition = 'temporary node to group expression entities up to ribbon terms'
                ON CREATE SET othergo.type = 'other'
                
        """

        AOExpression = """
            UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})

            MATCH (assay:MMOTerm:Ontology {primaryKey:row.assay})
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})
            
            WITH g, assay, otast, row

                MERGE (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                    SET e.whereExpressedStatement = row.whereExpressedStatement
                    
                MERGE (g)-[gex:EXPRESSED_IN]-(e)
                    SET gex.uuid = row.ei_uuid
                
                MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})
                    SET gej.joinType = 'expression',
                     gej.dataProviders = row.dataProviders
                
                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCIATION]->(gej)
                
                MERGE (gej)-[geja:ASSAY]-(assay)
        
                MERGE (e)-[gejotast:ANATOMICAL_STRUCTURE]-(otast)
                
                //MERGE (l:Load:Entity {primaryKey:row.loadKey})
                 //   SET l.dateProduced = row.dateProduced
                 //   SET l.loadName = "WT-Expression"
                //    SET l.dataProviders = row.dataProviders
                //    SET l.dataProvider = row.dataProvider
            
                //where only ao term exists
            
                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    SET pubf.pubModId = row.pubModId,
                     pubf.pubMedId = row.pubMedId,
                     pubf.pubModUrl = row.pubModUrl,
                     pubf.pubMedUrl = row.pubMedUrl

              //  MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) 
        """

        SGDCCExpression = """

        UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            MATCH (assay:MMOTerm:Ontology {primaryKey:row.assay})
            MATCH (otcct:GOTerm:Ontology {primaryKey:row.cellularComponentTermId})

            MERGE (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                    SET e.whereExpressedStatement = otcct.name

                MERGE (g)-[gex:EXPRESSED_IN]-(e)
                    SET gex.uuid = row.ei_uuid

                MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})
                    SET gej.joinType = 'expression',
                     gej.dataProviders = row.dataProviders

                MERGE (gej)-[geja:ASSAY]-(assay)

                MERGE (g)-[ggej:ASSOCIATION]->(gej)

                MERGE (e)-[egej:ASSOCIATION]->(gej)

                MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)

               // MERGE (l:Load:Entity {primaryKey:row.loadKey})
                //    SET l.dateProduced = row.dateProduced
                //    SET l.loadName = "WT-Expression"
                //    SET l.dataProviders = row.dataProviders
               //     SET l.dataProvider = row.dataProvider

                //where only ao term exists

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    SET pubf.pubModId = row.pubModId,
                     pubf.pubMedId = row.pubMedId,
                     pubf.pubModUrl = row.pubModUrl,
                     pubf.pubMedUrl = row.pubMedUrl

              //MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) 
        """

        CCExpression = """

        UNWIND $data as row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            MATCH (assay:MMOTerm:Ontology {primaryKey:row.assay})
            MATCH (otcct:GOTerm:Ontology {primaryKey:row.cellularComponentTermId})

            MERGE (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                    SET e.whereExpressedStatement = row.whereExpressedStatement
                    
                MERGE (g)-[gex:EXPRESSED_IN]-(e)
                    SET gex.uuid = row.ei_uuid
                             
                MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})
                    SET gej.joinType = 'expression',
                     gej.dataProviders = row.dataProviders
                
                MERGE (gej)-[geja:ASSAY]-(assay)

                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)
                
               // MERGE (l:Load:Entity {primaryKey:row.loadKey})
                //    SET l.dateProduced = row.dateProduced
                //    SET l.loadName = "WT-Expression"
                //    SET l.dataProviders = row.dataProviders
               //     SET l.dataProvider = row.dataProvider
                    
                //where only ao term exists

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    SET pubf.pubModId = row.pubModId,
                     pubf.pubMedId = row.pubMedId,
                     pubf.pubModUrl = row.pubModUrl,
                     pubf.pubMedUrl = row.pubMedUrl

              //MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) 
        """

        AOCCExpression = """
        
        UNWIND $data as row
            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            MATCH (assay:MMOTerm:Ontology {primaryKey:row.assay})
            MATCH (otcct:GOTerm:Ontology {primaryKey:row.cellularComponentTermId})
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId}) 

            WITH g, assay, otcct, otast, row WHERE NOT otast IS NULL AND NOT otcct IS NULL
                
   
                MERGE (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                    SET e.whereExpressedStatement = row.whereExpressedStatement
                
                MERGE (g)-[gex:EXPRESSED_IN]-(e)
                    SET gex.uuid = row.ei_uuid
                             
                MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})
                    SET gej.joinType = 'expression',
                     gej.dataProviders = row.dataProviders
                
                MERGE (gej)-[geja:ASSAY]-(assay)

                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCIATION]->(gej)
                
                
                MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)
                    
                MERGE (e)-[gejotast:ANATOMICAL_STRUCTURE]-(otast)
                    

               // MERGE (l:Load:Entity {primaryKey:row.loadKey})
                //    SET l.dateProduced = row.dateProduced
               //     SET l.loadName = "WT-Expression"
               //     SET l.dataProviders = row.dataProviders
               //     SET l.dataProvider = row.dataProvider
                    
                //where only ao term exists

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    SET pubf.pubModId = row.pubModId,
                     pubf.pubMedId = row.pubMedId,
                     pubf.pubModUrl = row.pubModUrl,
                     pubf.pubMedUrl = row.pubMedUrl

              //  MERGE (l)-[loadAssociation:LOADED_FROM]-(pubf)
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) 
        """

        EASSubstructure = """
                UNWIND $data as row
        
                    MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
                    MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                           
                    MERGE (e)-[eotasst:ANATOMICAL_SUB_SUBSTRUCTURE]->(otasst)                     
        """
        EASQualified = """
            UNWIND $data as row
                MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                
                MERGE (e)-[eotastq:ANATOMICAL_STRUCTURE_QUALIFIER]-(otastq)
            """
        EASSQualified = """
            UNWIND $data as row
                MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                
                MERGE (e)-[eotasstq:ANATOMICAL_SUB_STRUCTURE_QUALIFIER]-(otasstq)
            """
        CCQExpression = """  
            UNWIND $data as row
                MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                          
                MERGE (e)-[eotcctq:CELLULAR_COMPONENT_QUALIFIER]-(otcctq)      
        """

        stageExpression = """  
            UNWIND $data as row
                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MERGE (s:Stage {primaryKey:row.stageName})
                MERGE (ei)-[eotcctq:DURING]-(s)
        """

        uberonAO = """  
            UNWIND $data as row
                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid})  
                MATCH (o:UBERONTerm {primaryKey:row.aoUberonId})     
                MERGE (ebe)-[ebeo:ANATOMICAL_RIBBON_TERM]-(o)
        """

        uberonStage = """
            UNWIND $data as row
                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})  
                MATCH (o:UBERONTerm {primaryKey:row.uberonStageId})
                
                MERGE (ei)-[eio:STAGE_RIBBON_TERM]-(o)
        """

        uberonAOOther = """
            UNWIND $data as row
                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid}) 
                MATCH (u:UBERONTerm:Ontology {primaryKey:'UBERON:AnatomyOtherLocation'}) 
                
                MERGE (ebe)-[ebeu:ANATOMICAL_RIBBON_TERM]-(u)
        """

        uberonStageOther = """
            UNWIND $data as row
                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MATCH (u:UBERONTerm:Ontology {primaryKey:'UBERON:PostEmbryonicPreAdult'})
                
                MERGE (ei)-[eiu:STAGE_RIBBON_TERM]-(u)
            //TODO: get stage term ids from MGI
        """

        Transaction.execute_transaction(self, AddOther, "other")

        if species == 'Saccharomyces cerevisiae':
            if len(CCExpressionData) > 0:
                Transaction.execute_transaction(self, SGDCCExpression, CCExpressionData)

        else:
            if len(CCExpressionData) > 0:
                Transaction.execute_transaction(self, CCExpression, CCExpressionData)

        if len(AOExpressionData) > 0:
            Transaction.execute_transaction(self, AOExpression, AOExpressionData)

        if len(AOCCExpressionData) > 0 :
            Transaction.execute_transaction(self, AOCCExpression, AOCCExpressionData)

        if len(AOSubstructureData) > 0:
            Transaction.execute_transaction(self, EASSubstructure, AOSubstructureData)

        if len(AOQualifierData) > 0:
            Transaction.execute_transaction(self, EASQualified, AOQualifierData)

        if len(AOSSQualifierData) > 0:
            Transaction.execute_transaction(self, EASSQualified, AOSSQualifierData)

        if len(CCQualifierData) > 0:
            Transaction.execute_transaction(self, CCQExpression, CCQualifierData)

        if len(stageList) > 0:
            Transaction.execute_transaction(self, stageExpression, stageList)

        if len(uberonAOData) > 0:
            Transaction.execute_transaction(self, uberonAO, uberonAOData)

        if len(uberonAOOtherData) > 0:
            Transaction.execute_transaction(self, uberonAOOther, uberonAOOtherData)

        if len(stageUberonData) > 0:
            Transaction.execute_transaction(self, uberonStage, stageUberonData)

        if len(uberonStageOtherData) > 0:
            Transaction.execute_transaction(self, uberonStageOther, uberonStageOtherData)

        if len(xrefs) > 0:
            Transaction.execute_transaction(self, xref, xrefs)

    def retrieve_gocc_ribbon_terms(self):

        logger.info("reached the gocc_ribbon_txt code")

        expression_gocc_ribbon_retrieve = """
                MATCH (ebe:ExpressionBioEntity)--(go:GOTerm:Ontology)-[:PART_OF|IS_A*]->(slimTerm:GOTerm:Ontology) 
                where all (subset IN ['goslim_agr'] where subset in slimTerm.subset)
                return ebe.primaryKey, slimTerm.primaryKey
                """

        returnSet = Transaction.run_single_query(self, expression_gocc_ribbon_retrieve)

        gocc_ribbon_data = []

        for record in returnSet:
            row = dict(ebe_id=record["ebe.primaryKey"],
                       go_id=record["slimTerm.primaryKey"])
            gocc_ribbon_data.append(row)


        return gocc_ribbon_data

    def insert_gocc_ribbon_terms(self, gocc_ribbon_data):

        expression_gocc_ribbon_insert = """
                       UNWIND $data as row
                       
                       MATCH (ebe:ExpressionBioEntity) where ebe.primaryKey = row.ebe_id
                       MATCH (goTerm:GOTerm:Ontology) where goTerm.primaryKey = row.go_id

                       MERGE (ebe)-[ebego:CELLULAR_COMPONENT_RIBBON_TERM]-(slimTerm)
                       """

        Transaction.execute_transaction(self, expression_gocc_ribbon_insert, gocc_ribbon_data)

    def retrieve_gocc_ribbonless_ebes(self):

        ribbonless_ebes = """
            MATCH (ebe:ExpressionBioEntity)-[:CELLULAR_COMPONENT]-(goterm:GOTerm:Ontology)
            OPTIONAL MATCH (ebe)-[r:CELLULAR_COMPONENT_RIBBON_TERM]-(goribbon:GOTerm:Ontology)
            WHERE r is null
            RETURN ebe.primaryKey            
        """
        returnSet = Transaction.run_single_query(self, ribbonless_ebes)

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

        Transaction.execute_transaction(self, insert_ribbonless_data, gocc_ribbonless_data)
