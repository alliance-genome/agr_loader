'''Gene Disease Orthology ETL'''

import logging
import multiprocessing
import uuid

from datetime import datetime
from etl import ETL
from transactors import CSVTransactor, Neo4jTransactor
from .helpers import Neo4jHelper


class GeneDiseaseOrthoETL(ETL):
    '''Gene Disease Orthology ETL'''

    logger = logging.getLogger(__name__)

    insert_gene_disease_ortho_query = """
                USING PERIODIC COMMIT %s
                LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                
                MATCH (d:DOTerm:Ontology {primaryKey:row.doId}),
                  (gene:Gene {primaryKey:row.primaryId}),
                  (fromGene:Gene {primaryKey:row.fromGeneId}),
                  (pub:Publication {primaryKey:"MGI:6194238"}),
                  (ecode:ECOTerm {primaryKey:"ECO:0000501"})

                 CALL apoc.create.relationship(d, row.relationshipType, {}, gene) yield rel
                    SET rel.dataProvider = "Alliance",
                        rel.dateProduced = row.dateProduced,
                        rel.dateAssigned = row.dateAssigned
                    REMOVE rel.noOp

                MERGE (dga:Association:DiseaseEntityJoin {primaryKey:row.uuid})
                    ON CREATE SET dga.dataProvider = 'Alliance',
                                  dga.sortOrder = 10,
                                  dga.joinType = row.relationTypeLower

                CREATE (pubEJ:PublicationJoin:Association {primaryKey:row.pubEvidenceUuid})
                    SET pubEJ.joinType = 'pub_evidence_code_join',
                         pubEJ.dateProduced = row.dateProduced,
                        pubEJ.dateAssigned = row.dateAssigned
                        
                    
                MERGE (gene)-[fdag:ASSOCIATION]->(dga)
                MERGE (dga)-[dadg:ASSOCIATION]->(d)
                MERGE (dga)-[dapug:EVIDENCE]->(pubEJ)
                MERGE (dga)-[:FROM_ORTHOLOGOUS_GENE]->(fromGene)
                
                CREATE (pubEJ)-[pubEJecode1g:ASSOCIATION]->(ecode)
                CREATE (pub)-[pubgpubEJ:ASSOCIATION {uuid:row.pubEvidenceUuid}]->(pubEJ)
    """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config


    def _load_and_process_data(self):
        self.create_pub()

        thread_pool = []

        process = multiprocessing.Process(target=self._process, args=(self))
        process.start()
        thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

    def _process(self):

        self.logger.info("Starting Gene Disease Ortho Data")
        query_list = [
            [self.insert_gene_disease_ortho_query, "10000", "gene_disease_by_orthology.csv"]
        ]

        self.logger.info("gene disease ortho pub created")

        generators = self.retrieve_gene_disease_ortho()

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        self.logger.info("Finished Gene Disease Ortho Data")

    def create_pub(self):
        '''create publication'''

        self.logger.info("made it to the create pub for gene disease ortho")

        add_pub_query = """
        
              MERGE (pubg:Publication {primaryKey:"MGI:6194238"})
                  ON CREATE SET pubg.pubModId = "MGI:6194238",
                                pubg.pubModUrl = "http://www.informatics.jax.org/accession/MGI:6194238"
              MERGE (eco:ECOTerm:Ontology {primaryKey:"ECO:0000501"})
              
                    """

        self.logger.info("pub creation started")
        Neo4jHelper().run_single_query(add_pub_query)

        self.logger.info("pub creation finished")

    def retrieve_gene_disease_ortho(self):
        '''Retrieve Gene Disease Orthology'''

        self.logger.info("reached gene disease ortho retrieval")

        retrieve_gene_disease_ortho = """
            MATCH (disease:DOTerm)-[da:IS_IMPLICATED_IN|IS_MARKER_FOR]-(gene1:Gene)-[o:ORTHOLOGOUS]->(gene2:Gene)
            MATCH (ec:ECOTerm)-[ecpej:ASSOCIATION]-(pej:PublicationJoin:Association)-[:EVIDENCE]-(dej:DiseaseEntityJoin)-[a:ASSOCIATION]-(gene1:Gene)
                    WHERE o.strictFilter = true
                    AND da.uuid = dej.primaryKey
                    AND not (ec.primaryKey = "ECO:0000501")
                    AND not (ec.primaryKey = "ECO:0000250")
                    AND not (ec.primaryKey = "ECO:0000266")
                RETURN DISTINCT gene2.primaryKey AS geneID,
                    gene1.primaryKey AS fromGeneID,
                    type(da) AS relationType,
                    disease.primaryKey AS doId,
                    ec.primaryKey as ec
        """

        return_set = Neo4jHelper().run_single_query(retrieve_gene_disease_ortho)

        gene_disease_ortho_data = []
        relation_type = ""
        date = datetime.now()
        for record in return_set:
            if record['relationType'] == 'IS_IMPLICATED_IN':
                relation_type = 'IMPLICATED_VIA_ORTHOLOGY'
            elif record['relationType'] == 'IS_MARKER_FOR':
                relation_type = 'BIOMARKER_VIA_ORTHOLOGY'
            row = dict(primary_id=record["geneID"],
                       from_gene_id=record["fromGeneID"],
                       relationship_type=relation_type,
                       relation_type_lower=relation_type.lower(),
                       do_id=record["doId"],
                       date_produced=date,
                       date_assigned=date,
                       uuid=record["geneID"] + record["fromGeneID"] + relation_type + record["doId"],
                       pub_evidence_uuid=str(uuid.uuid4()))
            gene_disease_ortho_data.append(row)

        yield [gene_disease_ortho_data]
