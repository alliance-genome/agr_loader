import logging, uuid
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class OrthologyETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            //using match here to limit ortho set to genes that have already been loaded by bgi.
            MATCH(g1:Gene {primaryKey:row.gene1AgrPrimaryId})
            MATCH(g2:Gene {primaryKey:row.gene2AgrPrimaryId})
    
            CREATE (g1)-[orth:ORTHOLOGOUS]->(g2)
                SET orth.primaryKey = row.uuid,
                    orth.isBestScore = row.isBestScore,
                    orth.isBestRevScore = row.isBestRevScore,
                    orth.confidence = row.confidence,
                    orth.strictFilter = row.strictFilter,
                    orth.moderateFilter = row.moderateFilter
    
            //Create the Association node to be used for the object/doTerm
            CREATE (oa:Association:OrthologyGeneJoin {primaryKey:row.uuid})
                SET oa.joinType = 'orthologous'
            CREATE (g1)-[a1:ASSOCIATION]->(oa)
            CREATE (oa)-[a2:ASSOCIATION]->(g2) """

    algorithm_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (ogj:Association:OrthologyGeneJoin {primaryKey:row.uuid})
            MERGE (oa:OrthoAlgorithm {name:row.algorithm})
            WITH ogj, oa, row
                CALL apoc.create.relationship(ogj, row.reltype, {}, oa) YIELD rel
                RETURN rel """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []
        
        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        for thread in thread_pool:
            thread.join()
  
    def _process_sub_type(self, sub_type):
        logger.info("Loading Orthology Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)



        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()
        
        generators = self.get_generators(data, batch_size)

        query_list = [
            [OrthologyETL.query_template, commit_size, "orthology_data_" + sub_type.get_data_provider() + ".csv"],
            [OrthologyETL.algorithm_template, commit_size, "orthology_algorithm_data_" + sub_type.get_data_provider() + ".csv"],
        ]
            
        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        logger.info("Finished Loading Orthology Data: %s" % sub_type.get_data_provider())

    def get_generators(self, ortho_data, batch_size):

        counter = 0
        
        ortho_data_list = []
        algorithm_data = []

        dataProviderObject = ortho_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []
        dataProviders = []

        for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = ETLHelper.get_page_complete_url(dataProvider, ETL.xrefUrlMap, dataProvider, dataProviderPage)
                dataProviderCrossRefSet.append(
                    ETLHelper.get_xref_dict(dataProvider, dataProvider, dataProviderPage,
                                                  dataProviderPage, dataProvider, crossRefCompleteUrl, dataProvider + dataProviderPage))

        dataProviders.append(dataProvider)

        for orthoRecord in ortho_data['data']:

            # Sort out identifiers and prefixes.
            gene1 = ETLHelper.process_identifiers(orthoRecord['gene1'], dataProviders)
            # 'DRSC:'' removed, local ID, functions as display ID.
            gene2 = ETLHelper.process_identifiers(orthoRecord['gene2'], dataProviders)
            # 'DRSC:'' removed, local ID, functions as display ID.

            gene1Species = orthoRecord['gene1Species']
            gene2Species = orthoRecord['gene2Species']

            # Prefixed according to AGR prefixes.
            gene1AgrPrimaryId = ETLHelper.add_agr_prefix_by_species(gene1, gene1Species)

            # Prefixed according to AGR prefixes.
            gene2AgrPrimaryId = ETLHelper.add_agr_prefix_by_species(gene2, gene2Species)

            counter = counter + 1

            ortho_uuid = str(uuid.uuid4())

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(gene1AgrPrimaryId)
                is_it_test_entry2 = self.testObject.check_for_test_id_entry(gene2AgrPrimaryId)
                if is_it_test_entry is False and is_it_test_entry2 is False:
                    counter = counter - 1
                    continue

            if gene1AgrPrimaryId is not None and gene2AgrPrimaryId is not None:

                ortho_dataset = {
                    'isBestScore': orthoRecord['isBestScore'],
                    'isBestRevScore': orthoRecord['isBestRevScore'],

                    'gene1AgrPrimaryId': gene1AgrPrimaryId,
                    'gene2AgrPrimaryId': gene2AgrPrimaryId,

                    'confidence': orthoRecord['confidence'],

                    'strictFilter': orthoRecord['strictFilter'],
                    'moderateFilter': orthoRecord['moderateFilter'],
                    'uuid': ortho_uuid
                }
                ortho_data_list.append(ortho_dataset)

                for matched in orthoRecord.get('predictionMethodsMatched'):
                    matched_dataset = {
                        "uuid": ortho_uuid,
                        "algorithm": matched,
                        "reltype": "MATCHED"
                    }
                    algorithm_data.append(matched_dataset)

                for unmatched in orthoRecord.get('predictionMethodsNotMatched'):
                    unmatched_dataset = {
                        "uuid": ortho_uuid,
                        "algorithm": unmatched,
                        "reltype": "NOT_MATCHED"
                    }
                    algorithm_data.append(unmatched_dataset)

                for notcalled in orthoRecord.get('predictionMethodsNotCalled'):
                    notcalled_dataset = {
                        "uuid": ortho_uuid,
                        "algorithm": notcalled,
                        "reltype": "NOT_CALLED"
                    }
                    algorithm_data.append(notcalled_dataset)

                # Establishes the number of entries to yield (return) at a time.
                if counter == batch_size:
                    yield [ortho_data_list, algorithm_data]
                    ortho_data_list = []
                    algorithm_data = []
                    counter = 0

        if counter > 0:
            yield [ortho_data_list, algorithm_data]