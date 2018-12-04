import logging
logger = logging.getLogger(__name__)

import codecs
import uuid

import ijson

from etl import ETL
from etl.helpers import ETLHelper
from transactors import CSVTransactor

class ExpressionETL(ETL):


    xrefs_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid}) """ + ETLHelper.get_cypher_xref_text()

    AddOther = """
    
        MERGE(other:UBERONTerm:Ontology {primaryKey:'UBERON:AnatomyOtherLocation'})
            ON CREATE SET other.name = 'other'
        MERGE(otherstage:UBERONTerm:Ontology {primaryKey:'UBERON:PostEmbryonicPreAdult'})
            ON CREATE SET otherstage.name = 'post embryonic, pre-adult'
        MERGE(othergo:GOTerm:Ontology {primaryKey:'GO:otherLocations'})
            ON CREATE SET othergo.name = 'other locations'
            ON CREATE SET othergo.definition = 'temporary node to group expression entities up to ribbon terms'
            ON CREATE SET othergo.type = 'other'
            ON CREATE SET othergo.subset = 'goslim_agr' """

    AOExpression = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

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
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) """

    SGDCCExpression = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

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
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) """

    CCExpression = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

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
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) """

    AOCCExpression = """
        
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

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
                MERGE (gej)-[gejpubf:EVIDENCE]->(pubf) """

    EASSubstructure = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})       
            MERGE (e)-[eotasst:ANATOMICAL_SUB_SUBSTRUCTURE]->(otasst) """
        
    EASQualified = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
            MERGE (e)-[eotastq:ANATOMICAL_STRUCTURE_QUALIFIER]-(otastq) """
        
    EASSQualified = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
            
            MERGE (e)-[eotasstq:ANATOMICAL_SUB_STRUCTURE_QUALIFIER]-(otasstq) """
        
    CCQExpression = """  
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                      
            MERGE (e)-[eotcctq:CELLULAR_COMPONENT_QUALIFIER]-(otcctq) """

    stageExpression = """  
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
            MERGE (s:Stage {primaryKey:row.stageName})
            MERGE (ei)-[eotcctq:DURING]-(s) """

    uberonAO = """  
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid})  
            MATCH (o:UBERONTerm {primaryKey:row.aoUberonId})     
            MERGE (ebe)-[ebeo:ANATOMICAL_RIBBON_TERM]-(o) """

    uberonStage = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})  
            MATCH (o:UBERONTerm {primaryKey:row.uberonStageId})
            
            MERGE (ei)-[eio:STAGE_RIBBON_TERM]-(o) """

    uberonAOOther = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid}) 
            MATCH (u:UBERONTerm:Ontology {primaryKey:'UBERON:AnatomyOtherLocation'}) 
            MERGE (ebe)-[ebeu:ANATOMICAL_RIBBON_TERM]-(u) """

    uberonStageOther = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
            MATCH (u:UBERONTerm:Ontology {primaryKey:'UBERON:PostEmbryonicPreAdult'})
            
            MERGE (ei)-[eiu:STAGE_RIBBON_TERM]-(u) """


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        
        for sub_type in self.data_type_config.get_sub_type_objects():
            logger.info("Loading Expression Data: %s" % sub_type.get_data_provider())
            data_file = sub_type.get_filepath()
            logger.info("Finished Loading Expression Data: %s" % sub_type.get_data_provider())

            if data_file == None:
                logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
                continue

            commit_size = self.data_type_config.get_neo4j_commit_size()
            batch_size = self.data_type_config.get_generator_batch_size()

            # This needs to be in this format (template, param1, params2) others will be ignored
            query_list = [
                [ExpressionETL.xrefs_template, commit_size, "expression_crossReferences_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.AOExpression, commit_size, "expression_AOExpression_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.SGDCCExpression, commit_size, "expression_SGDCCExpression_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.CCExpression, commit_size, "expression_CCExpression_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.AOCCExpression, commit_size, "expression_AOCCExpression_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.EASSubstructure, commit_size, "expression_EASSubstructure_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.EASQualified, commit_size, "expression_EASQualified_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.EASSQualified, commit_size, "expression_EASSQualified_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.CCQExpression, commit_size, "expression_CCQExpression_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.stageExpression, commit_size, "expression_stageExpression_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.uberonAO, commit_size, "expression_uberonAO_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.uberonStage, commit_size, "expression_uberonStage_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.uberonAOOther, commit_size, "expression_uberonAOOther_" + sub_type.get_data_provider() + ".csv"],
                [ExpressionETL.uberonStageOther, commit_size, "expression_uberonStageOther_" + sub_type.get_data_provider() + ".csv"],
            ]

            #[aoExpression, ccExpression, aoQualifier, aoSubstructure, aoSSQualifier, ccQualifier,
            #    aoccExpression, stageList, stageUberonData, uberonAOData, uberonAOOtherData,
            #    uberonStageOtherData, crossReferences]

            # Obtain the generator
            generators = self.get_generators(data_file, batch_size)

            # Prepare the transaction
            CSVTransactor.execute_transaction(generators, query_list)


    def get_generators(self, expressionFile, batch_size):

        counter = 0
        crossReferences = []
        aoExpression = []
        ccExpression = []
        aoQualifier = []
        aoSubstructure = []
        aoSSQualifier = []
        ccQualifier = []
        aoccExpression = []
        stageList = []
        stageUberonData = []
        uberonAOData =[]
        uberonAOOtherData = []
        uberonStageOtherData = []

        logger.info("streaming json data from %s ..." % expressionFile)
        with codecs.open(expressionFile, 'r', 'utf-8') as f:

            for xpat in ijson.items(f, 'data.item'):
                counter = counter + 1

                pubMedUrl = None
                pubModUrl = None
                pubMedId = ""
                publicationModId = ""
                stageTermId = ""
                stageName = ""
                stageUberonTermId = ""
                geneId = xpat.get('geneId')

                if self.testObject.using_test_data() is True:
                    is_it_test_entry = self.testObject.check_for_test_id_entry(geneId)
                    if is_it_test_entry is False:
                        counter = counter - 1
                        continue

                evidence = xpat.get('evidence')

                if 'modPublicationId' in evidence:
                    publicationModId = evidence.get('modPublicationId')
                    if publicationModId is not None:
                        pubModLocalId = publicationModId.split(":")[1]
                        if "MGI:" in publicationModId:
                            pubModUrl = "http://www.informatics.jax.org/reference/" + publicationModId
                        if "ZFIN:" in publicationModId:
                            pubModUrl = "http://zfin.org/" + publicationModId
                        if "SGD:" in publicationModId:
                            pubModUrl = "https://www.yeastgenome.org/reference/" + pubModLocalId
                        if "WB:" in publicationModId:
                            pubModUrl = "https://www.wormbase.org/db/get?name=" + pubModLocalId + ";class=Paper"
                        if "RGD:" in publicationModId:
                            pubModUrl = "https://rgd.mcw.edu/rgdweb/report/reference/main.html?id=" + pubModLocalId
                        if "FB:" in publicationModId:
                            pubModUrl = "http://flybase.org/reports/" + pubModLocalId

                    if publicationModId is None:
                        publicationModId = ""

                if 'pubMedId' in evidence:
                    pubMedId = evidence.get('pubMedId')
                    localPubMedId = pubMedId.split(":")[1]
                    pubMedPrefix = pubMedId.split(":")[0]
                    pubMedUrl = ETLHelper.get_no_page_complete_url(localPubMedId, self.xrefUrlMap, pubMedPrefix, geneId)
                    if pubMedId is None:
                        pubMedId = ""

                assay = xpat.get('assay')
                ei_uuid = str(uuid.uuid4())
                ebe_uuid = str(uuid.uuid4())

                if 'crossReference' in xpat:
                    crossRef = xpat.get('crossReference')
                    crossRefId = crossRef.get('id')
                    local_crossref_id = crossRefId.split(":")[1]
                    prefix = crossRef.get('id').split(":")[0]
                    pages = crossRef.get('pages')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            if page == 'gene/expression/annotation/detail':
                                modGlobalCrossRefId = ETLHelper.get_page_complete_url(local_crossref_id, self.xrefUrlMap,
                                                                               prefix, page)

                                xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page, crossRefId,
                                                          modGlobalCrossRefId, crossRefId + page)
                                xref['ei_uuid'] = ei_uuid
                                crossReferences.append(xref)

                whenExpressedStage = xpat.get('whenExpressed')

                if 'stageTermId' in whenExpressedStage:
                    stageTermId = whenExpressedStage.get('stageTermId')
                if 'stageName' in whenExpressedStage:
                    stageName = whenExpressedStage.get('stageName')
                if whenExpressedStage.get('stageUberonSlimTerm') is not None:
                    stageUberonTermObject = whenExpressedStage.get('stageUberonSlimTerm')
                    stageUberonTermId = stageUberonTermObject.get("uberonTerm")
                    if stageUberonTermId is not None and stageUberonTermId != "post embryonic, pre-adult":
                        stageUberon = {
                            "uberonStageId": stageUberonTermId,
                            "ei_uuid": ei_uuid
                        }
                        stageUberonData.append(stageUberon)
                    if stageUberonTermId == "post embryonic, pre-adult":
                        stageUberonOther = {
                            "ei_uuid": ei_uuid
                        }
                        uberonStageOtherData.append(stageUberonOther)

                if stageTermId is None or stageName == 'N/A':
                    stageTermId = ""
                    stageName = ""
                    stageUberonTermId = ""

                if stageName is not None:

                    stage = {
                        "stageTermId": stageTermId,
                        "stageName": stageName,
                        "ei_uuid": ei_uuid
                    }

                    stageList.append(stage)

                else:
                    stageUberonTermId = ""

                if 'whereExpressed' in xpat:

                    whereExpressed = xpat.get('whereExpressed')
                    cellularComponentQualifierTermId = whereExpressed.get('cellularComponentQualifierTermId')
                    cellularComponentTermId = whereExpressed.get('cellularComponentTermId')
                    anatomicalStructureTermId = whereExpressed.get('anatomicalStructureTermId')
                    anatomicalStructureQualifierTermId = whereExpressed.get(
                        'anatomicalStructureQualifierTermId')
                    anatomicalSubStructureTermId = whereExpressed.get('anatomicalSubStructureTermId')
                    anatomicalSubStructureQualifierTermId = whereExpressed.get(
                        'anatomicalSubStructureQualifierTermId')
                    whereExpressedStatement = whereExpressed.get('whereExpressedStatement')

                    if whereExpressed.get('anatomcialStructureUberonSlimTermIds') is not None:
                        for uberonStructureTermObject in whereExpressed.get('anatomcialStructureUberonSlimTermIds'):
                            structureUberonTermId = uberonStructureTermObject.get('uberonTerm')
                            if structureUberonTermId is not None and structureUberonTermId != 'Other':
                                structureUberonTerm = {
                                    "ebe_uuid": ebe_uuid,
                                    "aoUberonId": structureUberonTermId
                                }
                                uberonAOData.append(structureUberonTerm)
                            elif structureUberonTermId is not None and structureUberonTermId == 'Other':
                                otherStructureUberonTerm = {
                                    "ebe_uuid": ebe_uuid
                                }
                                uberonAOOtherData.append(otherStructureUberonTerm)

                    if whereExpressed.get('anatomicalSubStructureUberonSlimTermIds') is not None:
                        for uberonSubStructureTermObject in whereExpressed.get('anatomicalSubStructureUberonSlimTermIds'):
                            subStructureUberonTermId = uberonSubStructureTermObject.get('uberonTerm')
                            if subStructureUberonTermId is not None and subStructureUberonTermId != 'Other':
                                subStructureUberonTerm = {
                                    "ebe_uuid": ebe_uuid,
                                    "aoUberonId": subStructureUberonTermId
                                }
                                uberonAOData.append(subStructureUberonTerm)
                            elif subStructureUberonTermId is not None and subStructureUberonTermId == 'Other':
                                otherStructureUberonTerm = {
                                    "ebe_uuid": ebe_uuid
                                }
                                uberonAOOtherData.append(otherStructureUberonTerm)

                    if cellularComponentTermId is None:
                        cellularComponentTermId = ""

                        AOExpression = {
                            "geneId": geneId,
                            "whenExpressedStage": whenExpressedStage,
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": publicationModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "uuid": str(uuid.uuid4()),
                            "assay": assay,
                            "anatomicalStructureTermId": anatomicalStructureTermId,
                            "whereExpressedStatement": whereExpressedStatement,
                            "ei_uuid": ei_uuid,
                            "ebe_uuid": ebe_uuid

                        }
                        aoExpression.append(AOExpression)

                    if cellularComponentQualifierTermId is not None:

                        CCQualifier = {
                            "ebe_uuid": ebe_uuid,
                            "cellularComponentQualifierTermId": cellularComponentQualifierTermId

                        }
                        ccQualifier.append(CCQualifier)

                    if anatomicalStructureTermId is None:
                        anatomicalStructureTermId = ""

                        CCExpression = {
                            "geneId": geneId,
                            "whenExpressedStage": whenExpressedStage,
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": publicationModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "assay": assay,
                            "whereExpressedStatement": whereExpressedStatement,
                            "cellularComponentTermId": cellularComponentTermId,
                            "ei_uuid": ei_uuid,
                            "ebe_uuid": ebe_uuid
                        }
                        ccExpression.append(CCExpression)

                    if anatomicalStructureQualifierTermId is not None:
                        AOQualifier = {
                            "ebe_uuid": ebe_uuid,
                            "anatomicalStructureQualifierTermId": anatomicalStructureQualifierTermId
                        }
                        aoQualifier.append(AOQualifier)

                    if anatomicalSubStructureTermId is not None:
                        AOSubstructure = {
                            "ebe_uuid": ebe_uuid,
                            "anatomicalSubStructureTermId": anatomicalSubStructureTermId

                        }
                        aoSubstructure.append(AOSubstructure)

                    if anatomicalSubStructureQualifierTermId is not None:
                        AOSSQualifier = {
                            "ebe_uuid": ebe_uuid,
                            "anatomicalSubStructureQualifierTermId": anatomicalSubStructureQualifierTermId

                        }
                        aoSSQualifier.append(AOSSQualifier)

                    if whereExpressedStatement is None:
                        whereExpressedStatement = ""

                    if anatomicalStructureTermId is not None and anatomicalStructureTermId != "" \
                            and cellularComponentTermId is not None and cellularComponentTermId != "":

                        AOCCExpression = {
                            "geneId": geneId,
                            "whenExpressedStage": whenExpressedStage,
                            "pubMedId": pubMedId,
                            "pubMedUrl": pubMedUrl,
                            "pubModId": publicationModId,
                            "pubModUrl": pubModUrl,
                            "pubPrimaryKey": pubMedId + publicationModId,
                            "uuid": str(uuid.uuid4()),
                            "stageTermId": stageTermId,
                            "stageName": stageName,
                            "stageUberonTermId": stageUberonTermId,
                            "assay": assay,
                            "cellularComponentTermId": cellularComponentTermId,
                            "anatomicalStructureTermId": anatomicalStructureTermId,
                            "whereExpressedStatement": whereExpressedStatement,
                            "ei_uuid": ei_uuid,
                            "ebe_uuid": ebe_uuid
                        }

                        aoccExpression.append(AOCCExpression)

                if counter == batch_size:
                    counter = 0
                    logger.debug("counter equals batch size")
                    yield [aoExpression, ccExpression, aoQualifier, aoSubstructure, aoSSQualifier, ccQualifier,
                           aoccExpression, stageList, stageUberonData, uberonAOData, uberonAOOtherData,
                           uberonStageOtherData, crossReferences]
                    aoExpression = []
                    ccExpression = []
                    aoQualifier = []
                    aoSubstructure = []
                    aoSSQualifier = []
                    ccQualifier = []
                    aoccExpression = []
                    stageList = []
                    uberonStageOtherData = []
                    stageUberonData = []
                    uberonAOOtherData = []
                    uberonAOData = []
                    crossReferences = []
                    #counter = 0

            if counter > 0:
                yield [aoExpression, ccExpression, aoQualifier, aoSubstructure, aoSSQualifier, ccQualifier,
                       aoccExpression, stageList, stageUberonData, uberonAOData, uberonAOOtherData,
                       uberonStageOtherData, crossReferences]

        # TODO: get dataProvider parsing working with ijson.
        # wt_expression_data = JSONFile().get_data(path + expressionName, 'expression')
        #
        # dateProduced = wt_expression_data['metaData']['dateProduced']
        # for dataProviderObject in wt_expression_data['metaData']['dataProvider']:
        #
        #     dataProviderCrossRef = dataProviderObject.get('crossReference')
        #     dataProviderType = dataProviderObject.get('type')
        #     dataProvider = dataProviderCrossRef.get('id')
        #     dataProviderPages = dataProviderCrossRef.get('pages')
        #     dataProviderCrossRefSet = []
        #     dataProviders = []
        #     loadKey = loadKey + dateProduced + dataProvider + "_BGI"
        #
        #     for dataProviderPage in dataProviderPages:
        #         crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, xrefUrlMap, dataProvider,
        #                                                                dataProviderPage)
        #         dataProviderCrossRefSet.append(
        #             CreateCrossReference.get_xref(dataProvider, dataProvider, dataProviderPage,
        #                                           dataProviderPage, dataProvider, crossRefCompleteUrl,
        #                                           dataProvider + dataProviderPage))
        #
        #     dataProviders.append(dataProvider)
        #
        # dataProviderSingle = DataProvider().get_data_provider(species)
        # logger.info ("dataProvider found: " + dataProviderSingle)
