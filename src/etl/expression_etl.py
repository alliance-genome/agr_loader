import logging
import codecs
import uuid
import ijson, multiprocessing

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor
logger = logging.getLogger(__name__)


class ExpressionETL(ETL):


    xrefs_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid}) """ + ETLHelper.get_cypher_xref_text()


    BioEntityExpression = """
    
    USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
    
    MERGE (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
         ON CREATE SET e.whereExpressedStatement = row.whereExpressedStatement
    
    """

    BioEntityGeneExpressionJoin = """
    
     USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
    
    MERGE (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})
         ON CREATE SET gej.joinType = 'expression'
    
    """

    AOExpression = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})

            MATCH (assay:MMOTerm:Ontology {primaryKey:row.assay})
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId}) 
                WHERE NOT 'UBERONTerm' in LABELS(otast)
                AND NOT 'FBCVTerm' in LABELS(otast)
                
            MATCH (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})  
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                    
                MERGE (g)-[gex:EXPRESSED_IN]->(e)
                    ON CREATE SET gex.uuid = row.ei_uuid
                
                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCIATION]->(gej)
                
                MERGE (gej)-[geja:ASSAY]->(assay)
        
                MERGE (e)-[gejotast:ANATOMICAL_STRUCTURE]->(otast)

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    SET pubf.pubModId = row.pubModId,
                     pubf.pubMedId = row.pubMedId,
                     pubf.pubModUrl = row.pubModUrl,
                     pubf.pubMedUrl = row.pubMedUrl

                CREATE (gej)-[gejpubf:EVIDENCE]->(pubf) """

    SGDCCExpression = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            MATCH (assay:MMOTerm:Ontology {primaryKey:row.assay})
            MATCH (otcct:GOTerm:Ontology {primaryKey:row.cellularComponentTermId})

            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
            MATCH (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})

                MERGE (g)-[gex:EXPRESSED_IN]->(e)
                    ON CREATE SET gex.uuid = row.ei_uuid
                MERGE (gej)-[geja:ASSAY]->(assay)

                MERGE (g)-[ggej:ASSOCIATION]->(gej)

                MERGE (e)-[egej:ASSOCIATION]->(gej)

                MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    ON CREATE SET pubf.pubModId = row.pubModId,
                     pubf.pubMedId = row.pubMedId,
                     pubf.pubModUrl = row.pubModUrl,
                     pubf.pubMedUrl = row.pubMedUrl

                CREATE (gej)-[gejpubf:EVIDENCE]->(pubf) """

    CCExpression = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            MATCH (assay:MMOTerm:Ontology {primaryKey:row.assay})
            MATCH (otcct:GOTerm:Ontology {primaryKey:row.cellularComponentTermId})

            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
            MATCH (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})         
               
                MERGE (g)-[gex:EXPRESSED_IN]->(e)
                    ON CREATE SET gex.uuid = row.ei_uuid

                
                MERGE (gej)-[geja:ASSAY]->(assay)

                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    ON CREATE SET pubf.pubModId = row.pubModId,
                     pubf.pubMedId = row.pubMedId,
                     pubf.pubModUrl = row.pubModUrl,
                     pubf.pubMedUrl = row.pubMedUrl

                CREATE (gej)-[gejpubf:EVIDENCE]->(pubf) """

    AOCCExpression = """
        
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            // GET PRIMARY DATA OBJECTS

            // LOAD NODES
            MATCH (g:Gene {primaryKey:row.geneId})
            MATCH (assay:MMOTerm:Ontology {primaryKey:row.assay})
            MATCH (otcct:GOTerm:Ontology {primaryKey:row.cellularComponentTermId})
            MATCH (otast:Ontology {primaryKey:row.anatomicalStructureTermId})                 
                WHERE NOT 'UBERONTerm' in LABELS(otast)
                    AND NOT 'FBCVTerm' in LABELS(otast)
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
            MATCH (gej:BioEntityGeneExpressionJoin:Association {primaryKey:row.ei_uuid})   
                
            WITH g, e, gej, assay, otcct, otast, row WHERE NOT otast IS NULL AND NOT otcct IS NULL
                
   
                MERGE (g)-[gex:EXPRESSED_IN]->(e)
                    ON CREATE SET gex.uuid = row.ei_uuid
                            
                
                MERGE (gej)-[geja:ASSAY]->(assay)

                MERGE (g)-[ggej:ASSOCIATION]->(gej)
                    
                MERGE (e)-[egej:ASSOCIATION]->(gej)
                
                
                MERGE (e)-[eotcct:CELLULAR_COMPONENT]->(otcct)
                    
                MERGE (e)-[gejotast:ANATOMICAL_STRUCTURE]-(otast)

                MERGE (pubf:Publication {primaryKey:row.pubPrimaryKey})
                    ON CREATE SET pubf.pubModId = row.pubModId,
                     pubf.pubMedId = row.pubMedId,
                     pubf.pubModUrl = row.pubModUrl,
                     pubf.pubMedUrl = row.pubMedUrl

                CREATE (gej)-[gejpubf:EVIDENCE]->(pubf) """

    EASSubstructure = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
                WHERE NOT 'UBERONTerm' in LABELS(otasst)
                    AND NOT 'FBCVTerm' in LABELS(otasst)
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})       
            MERGE (e)-[eotasst:ANATOMICAL_SUB_SUBSTRUCTURE]->(otasst) """
        
    EASQualified = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
                WHERE NOT 'UBERONTerm' in LABELS(otastq)
                    AND NOT 'FBCVTerm' in LABELS(otastq)
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
            MERGE (e)-[eotastq:ANATOMICAL_STRUCTURE_QUALIFIER]-(otastq) """
        
    EASSQualified = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})
                WHERE NOT 'UBERONTerm' in LABELS(otasstq)
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
            
            MERGE (e)-[eotasstq:ANATOMICAL_SUB_STRUCTURE_QUALIFIER]-(otasstq) """
        
    CCQExpression = """  
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
                WHERE NOT 'UBERONTerm' in LABELS(otcctq)
            MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                      
            MERGE (e)-[eotcctq:CELLULAR_COMPONENT_QUALIFIER]-(otcctq) """

    stageExpression = """  
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
            MERGE (s:Stage {primaryKey:row.stageName})
                ON CREATE SET s.name = row.stageName
            MERGE (ei)-[eotcctq:DURING]-(s) """

    uberonAO = """  
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid})  
            MATCH (o:Ontology:UBERONTerm {primaryKey:row.aoUberonId})     
            MERGE (ebe)-[ebeo:ANATOMICAL_RIBBON_TERM]-(o) """

    uberonStage = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})  
            MATCH (o:Ontology:UBERONTerm {primaryKey:row.uberonStageId})
            
            MERGE (ei)-[eio:STAGE_RIBBON_TERM]-(o) """

    uberonAOOther = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid}) 
            MATCH (u:Ontology:UBERONTerm:Ontology {primaryKey:'UBERON:AnatomyOtherLocation'}) 
            MERGE (ebe)-[ebeu:ANATOMICAL_RIBBON_TERM]-(u) """

    uberonStageOther = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
            MATCH (u:Ontology:UBERONTerm:Ontology {primaryKey:'UBERON:PostEmbryonicPreAdult'})
            
            MERGE (ei)-[eiu:STAGE_RIBBON_TERM]-(u) """


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config
    
    
    def _load_and_process_data(self):
        # add the 'other' nodes to support the expression ribbon components.
        self.add_other()
        
        thread_pool = []
        query_tracking_list = multiprocessing.Manager().list()
        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type, query_tracking_list))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

        queries = []
        for item in query_tracking_list:
            queries.append(item)
            
        Neo4jTransactor.execute_query_batch(queries)
            
    def _process_sub_type(self, sub_type, query_tracking_list):

        logger.info("Loading Expression Data: %s" % sub_type.get_data_provider())
        data_file = sub_type.get_filepath()
        data_provider = sub_type.get_data_provider()

        if data_file is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored

        query_list = [
                      [ExpressionETL.BioEntityExpression, commit_size, "expression_entities_" + sub_type.get_data_provider() + ".csv"],
                      [ExpressionETL.BioEntityGeneExpressionJoin, commit_size, "expression_entity_joins_" + sub_type.get_data_provider() + ".csv"],
                      [ExpressionETL.AOExpression, commit_size, "expression_AOExpression_" + sub_type.get_data_provider() + ".csv"]
        ]

        if data_provider == 'SGD':
            query_list += [[ExpressionETL.SGDCCExpression, commit_size,  "expression_SGDCCExpression_" + sub_type.get_data_provider() + ".csv"]]
        else:
            query_list += [[ExpressionETL.CCExpression, commit_size, "expression_CCExpression_" + sub_type.get_data_provider() + ".csv"]]
            
        query_list += [
            [ExpressionETL.AOCCExpression, commit_size, "expression_AOCCExpression_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.EASQualified, commit_size, "expression_EASQualified_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.EASSubstructure, commit_size, "expression_EASSubstructure_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.EASSQualified, commit_size, "expression_EASSQualified_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.CCQExpression, commit_size, "expression_CCQExpression_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.stageExpression, commit_size, "expression_stageExpression_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.uberonStage, commit_size, "expression_uberonStage_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.uberonAO, commit_size, "expression_uberonAO_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.uberonAOOther, commit_size, "expression_uberonAOOther_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.uberonStageOther, commit_size, "expression_uberonStageOther_" + sub_type.get_data_provider() + ".csv"],
            [ExpressionETL.xrefs_template, commit_size, "expression_crossReferences_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data_file, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        
        for item in query_and_file_list:
            query_tracking_list.append(item)
        
        logger.info("Finished Loading Expression Data: %s" % sub_type.get_data_provider())

    def add_other(self):

        logger.debug("made it to the addOther statement")

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

        Neo4jHelper.run_single_query(AddOther)


    def get_generators(self, expressionFile, batch_size):
        logger.debug("made it to the expression generator")
        counter = 0
        crossReferences = []
        bioEntities = []
        bioJoinEntities = []
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

        logger.debug("streaming json data from %s ..." % expressionFile)
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

                    # TODO: making unique BioEntityGeneExpressionJoin nodes and ExpressionBioEntity nodes is tedious.
                    # TODO: Lets get the DQMs to fix this.
                    expressionUniqueKey = geneId + assay + stageName
                    expressionEntityUniqueKey = ""

                    if anatomicalStructureTermId is not None:
                        expressionUniqueKey = expressionUniqueKey + anatomicalStructureTermId
                        expressionEntityUniqueKey = anatomicalStructureTermId

                        if anatomicalStructureQualifierTermId is not None:
                            expressionUniqueKey = expressionUniqueKey + anatomicalStructureQualifierTermId
                            expressionEntityUniqueKey = expressionEntityUniqueKey + anatomicalStructureQualifierTermId

                    if cellularComponentTermId is not None:
                        expressionUniqueKey = expressionUniqueKey + cellularComponentTermId
                        expressionEntityUniqueKey = expressionEntityUniqueKey + cellularComponentTermId

                        if cellularComponentQualifierTermId is not None:
                            expressionUniqueKey = expressionUniqueKey + cellularComponentQualifierTermId
                            expressionEntityUniqueKey = expressionEntityUniqueKey + cellularComponentQualifierTermId

                    if anatomicalSubStructureTermId is not None:
                        expressionUniqueKey = expressionUniqueKey + anatomicalSubStructureTermId

                        if anatomicalSubStructureQualifierTermId is not None:
                            expressionUniqueKey = expressionUniqueKey + anatomicalSubStructureQualifierTermId
                            expressionEntityUniqueKey = expressionEntityUniqueKey + anatomicalSubStructureQualifierTermId

                    expressionEntityUniqueKey = expressionEntityUniqueKey + whereExpressedStatement

                    whenExpressedStage = xpat.get('whenExpressed')

                    if 'stageTermId' in whenExpressedStage:
                        stageTermId = whenExpressedStage.get('stageTermId')
                    if 'stageName' in whenExpressedStage:
                        stageName = whenExpressedStage.get('stageName')

                    if whereExpressed.get('anatomcialStructureUberonSlimTermIds') is not None:
                        for uberonStructureTermObject in whereExpressed.get('anatomcialStructureUberonSlimTermIds'):
                            structureUberonTermId = uberonStructureTermObject.get('uberonTerm')
                            if structureUberonTermId is not None and structureUberonTermId != 'Other':
                                structureUberonTerm = {
                                    "ebe_uuid": expressionEntityUniqueKey,
                                    "aoUberonId": structureUberonTermId
                                }
                                uberonAOData.append(structureUberonTerm)
                            elif structureUberonTermId is not None and structureUberonTermId == 'Other':
                                otherStructureUberonTerm = {
                                    "ebe_uuid": expressionEntityUniqueKey
                                }
                                uberonAOOtherData.append(otherStructureUberonTerm)

                    if whereExpressed.get('anatomicalSubStructureUberonSlimTermIds') is not None:
                        for uberonSubStructureTermObject in whereExpressed.get('anatomicalSubStructureUberonSlimTermIds'):
                            subStructureUberonTermId = uberonSubStructureTermObject.get('uberonTerm')
                            if subStructureUberonTermId is not None and subStructureUberonTermId != 'Other':
                                subStructureUberonTerm = {
                                    "ebe_uuid": expressionEntityUniqueKey,
                                    "aoUberonId": subStructureUberonTermId
                                }
                                uberonAOData.append(subStructureUberonTerm)
                            elif subStructureUberonTermId is not None and subStructureUberonTermId == 'Other':
                                otherStructureUberonTerm = {
                                    "ebe_uuid": expressionEntityUniqueKey
                                }
                                uberonAOOtherData.append(otherStructureUberonTerm)

                    if cellularComponentTermId is None:
                        cellularComponentTermId = ""




                    if whenExpressedStage.get('stageUberonSlimTerm') is not None:
                        stageUberonTermObject = whenExpressedStage.get('stageUberonSlimTerm')
                        stageUberonTermId = stageUberonTermObject.get("uberonTerm")
                        if stageUberonTermId is not None and stageUberonTermId != "post embryonic, pre-adult":
                            stageUberon = {
                                "uberonStageId": stageUberonTermId,
                                "ei_uuid": expressionUniqueKey
                            }
                            stageUberonData.append(stageUberon)
                        if stageUberonTermId == "post embryonic, pre-adult":
                            stageUberonOther = {
                                "ei_uuid": expressionUniqueKey
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
                            "ei_uuid": expressionUniqueKey
                        }

                        stageList.append(stage)

                    else:
                        stageUberonTermId = ""


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
                                    modGlobalCrossRefId = ETLHelper.get_page_complete_url(local_crossref_id,
                                                                                              self.xrefUrlMap,
                                                                                              prefix, page)

                                    xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page,
                                                                       crossRefId,
                                                                       modGlobalCrossRefId, crossRefId + page)
                                    xref['ei_uuid'] = expressionUniqueKey
                                    crossReferences.append(xref)

                    BioEntity = {
                        "ebe_uuid": expressionEntityUniqueKey,
                        "whereExpressedStatement": whereExpressedStatement
                    }
                    bioEntities.append(BioEntity)

                    BioEntityJoin = {
                        "ei_uuid": expressionUniqueKey
                    }

                    bioJoinEntities.append(BioEntityJoin)

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
                        "ei_uuid": expressionUniqueKey,
                        "ebe_uuid": expressionEntityUniqueKey

                    }
                    aoExpression.append(AOExpression)

                    if cellularComponentQualifierTermId is not None:

                        CCQualifier = {
                            "ebe_uuid": expressionEntityUniqueKey,
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
                            "ei_uuid": expressionUniqueKey,
                            "ebe_uuid": expressionEntityUniqueKey
                        }
                        ccExpression.append(CCExpression)


                    if anatomicalStructureQualifierTermId is not None:
                        AOQualifier = {
                            "ebe_uuid": expressionEntityUniqueKey,
                            "anatomicalStructureQualifierTermId": anatomicalStructureQualifierTermId
                        }
                        aoQualifier.append(AOQualifier)

                    if anatomicalSubStructureTermId is not None:
                        AOSubstructure = {
                            "ebe_uuid": expressionEntityUniqueKey,
                            "anatomicalSubStructureTermId": anatomicalSubStructureTermId

                        }
                        aoSubstructure.append(AOSubstructure)

                    if anatomicalSubStructureQualifierTermId is not None:
                        AOSSQualifier = {
                            "ebe_uuid": expressionEntityUniqueKey,
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
                            "ei_uuid": expressionUniqueKey,
                            "ebe_uuid": expressionEntityUniqueKey
                        }

                        aoccExpression.append(AOCCExpression)

                if counter == batch_size:
                    yield [bioEntities, bioJoinEntities, aoExpression, ccExpression, aoccExpression, aoQualifier, aoSubstructure,
                           aoSSQualifier, ccQualifier,
                           stageList, stageUberonData, uberonAOData, uberonAOOtherData,
                           uberonStageOtherData, crossReferences]
                    bioEntities = []
                    bioJoinEntities = []
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
                    counter = 0

            if counter > 0:
                yield [bioEntities, bioJoinEntities, aoExpression, ccExpression, aoccExpression, aoQualifier, aoSubstructure, aoSSQualifier, ccQualifier,
                       stageList, stageUberonData, uberonAOData, uberonAOOtherData,
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
