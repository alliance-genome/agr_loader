import logging
logger = logging.getLogger(__name__)
from etl import ETL
from .helpers import Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor
import multiprocessing
import uuid, logging
from datetime import datetime

class GeneDiseaseOrthoETL(ETL):

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    insert_gene_disease_ortho = """
                USING PERIODIC COMMIT %s
                LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                
                MATCH (d:DOTerm:Ontology {primaryKey:row.doId}),
                  (gene:Gene {primaryKey:row.primaryId}),
                  (fromGene:Gene {primaryKey:row.fromGeneId}),
                  (pub:Publication {primaryKey:"MGI:6194238"}),
                  (ecode:ECOTerm {primaryKey:"ECO:0000501"})

                MERGE (dga:Association:DiseaseEntityJoin {primaryKey:row.uuid})
                    SET dga.dataProvider = 'Alliance',
                                  dga.sortOrder = 10

                FOREACH (rel IN CASE when row.relationshipType = 'IS_MARKER_FOR' THEN [1] ELSE [] END |
                    CREATE (gene)-[fafg:BIOMARKER_VIA_ORTHOLOGY {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = "Alliance",
                        fafg.dateProduced = row.dateProduced,
                        dga.joinType = 'biomarker_via_orthology')

                FOREACH (rel IN CASE when row.relationshipType = 'IS_IMPLICATED_IN' THEN [1] ELSE [] END |
                    CREATE (gene)-[fafg:IMPLICATED_VIA_ORTHOLOGY {uuid:row.uuid}]->(d)
                    SET fafg.dataProvider = "Alliance",
                        fafg.dateProduced = row.dateProduced,
                        dga.joinType = 'implicated_via_orthology')

                CREATE (pubEJ:PublicationEvidenceCodeJoin:Association {primaryKey:row.pubEvidenceUuid})
                    SET pubEJ.joinType = 'pub_evidence_code_join'
                    
                MERGE (gene)-[fdag:ASSOCIATION]->(dga)
                MERGE (dga)-[dadg:ASSOCIATION]->(d)
                MERGE (dga)-[dapug:EVIDENCE]->(pubEJ)
                MERGE (dga)-[:FROM_ORTHOLOGOUS_GENE]->(fromGene)
                
                CREATE (pubEJ)-[pubEJecode1g:ASSOCIATION]->(ecode)
                CREATE (pub)-[pubgpubEJ:ASSOCIATION {uuid:row.pubEvidenceUuid}]->(pubEJ)
    """


    def _load_and_process_data(self):
        self.create_pub()

        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, subtype):

        logger.info("Starting Gene Disease Ortho Data")
        query_list = [
            [self.insert_gene_disease_ortho, "10000",
             "gene_disease_by_orthology.csv"]
        ]

        logger.info("gene disease ortho pub created")

        generators = self.retrieve_gene_disease_ortho()

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        
        logger.info("Finished Gene Disease Ortho Data")

    def create_pub(self):

        logger.info("made it to the create pub for gene disease ortho")

        addPub = """              
        
              MERGE (pubg:Publication {primaryKey:"MGI:6194238"})
                  ON CREATE SET pubg.pubModId = "MGI:6194238",
                                pubg.pubModUrl = "http://www.informatics.jax.org/accession/MGI:6194238"
              MERGE (:ECOTerm {primaryKey:"ECO:0000501"})
              
                    """

        logger.info("pub creation started")
        Neo4jHelper().run_single_query(addPub)

        logger.info("pub creation finished")

    def retrieve_gene_disease_ortho(self):
        logger.info("reached gene disease ortho retrieval")

        retrieve_gene_disease_ortho = """
           MATCH (disease:DOTerm)-[da:IS_IMPLICATED_IN|IS_MARKER_FOR]-(gene1:Gene)-[o:ORTHOLOGOUS]->(gene2:Gene)
            MATCH (ec:ECOTerm)-[ecpej:ASSOCIATION]-(pej:PublicationEvidenceCodeJoin)-[:EVIDENCE]-(dej:DiseaseEntityJoin)-[a:ASSOCIATION]-(gene1:Gene)-[:FROM_SPECIES]->(species:Species)
                with collect(distinct ec.primaryKey) as evCodes, o, da, dej, ec, gene1, gene2, disease
                    WHERE o.strictFilter
                    AND da.uuid = dej.primaryKey
                    AND (any(x IN evCodes where NOT x in ["ECO:0000501", "ECO:0000250", "ECO:0000266"]))
                RETURN DISTINCT gene2.primaryKey AS geneID,
                    gene1.primaryKey AS fromGeneID,
                    type(da) AS relationType,
                    disease.primaryKey AS doId
        """

        returnSet = Neo4jHelper().run_single_query(retrieve_gene_disease_ortho)

        gene_disease_ortho_data = []

        for record in returnSet:
            row = dict(primaryId=record["geneID"],
                    fromGeneId=record["fromGeneID"],
                    relationshipType=record["relationType"],
                    doId=record["doId"],
                    dateProduced=datetime.now(),
                    uuid=record["geneID"]+record["relationType"]+record["doId"],
                    pubEvidenceUuid=str(uuid.uuid4()))
            gene_disease_ortho_data.append(row)

        yield [gene_disease_ortho_data]
