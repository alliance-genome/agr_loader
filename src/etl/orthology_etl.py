import logging, uuid
logger = logging.getLogger(__name__)

from transactors import CSVTransactor

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile

class OrthologyETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            //using match here to limit ortho set to genes that have already been loaded by bgi.
            MATCH(g1:Gene {primaryKey:row.gene1AgrPrimaryId})
            MATCH(g2:Gene {primaryKey:row.gene2AgrPrimaryId})
    
            MERGE (g1)-[orth:ORTHOLOGOUS {primaryKey:row.uuid}]->(g2)
                SET orth.isBestScore = row.isBestScore,
                 orth.isBestRevScore = row.isBestRevScore,
                 orth.confidence = row.confidence,
                 orth.strictFilter = row.strictFilter,
                 orth.moderateFilter = row.moderateFilter
    
            //Create the Association node to be used for the object/doTerm
            MERGE (oa:Association:OrthologyGeneJoin {primaryKey:row.uuid})
                SET oa.joinType = 'orthologous'
            MERGE (g1)-[a1:ASSOCIATION]->(oa)
            MERGE (oa)-[a2:ASSOCIATION]->(g2)

    """

    matched_algorithm_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (oa:Association:OrthologyGeneJoin {primaryKey:row.uuid})
            MERGE (match:OrthoAlgorithm {name:row.algorithm})
                MERGE (oa)-[m:MATCHED]->(match)
    """

    unmatched_algorithm_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (oa:Association:OrthologyGeneJoin {primaryKey:row.uuid})
            MERGE (notmatch:OrthoAlgorithm {name:row.algorithm})
                MERGE (oa)-[m:NOT_MATCHED]->(notmatch)
    """

    not_called_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (oa:Association:OrthologyGeneJoin {primaryKey:row.uuid})
            MERGE (notcalled:OrthoAlgorithm {name:row.algorithm})
                MERGE (oa)-[m:NOT_CALLED]->(notcalled)
    """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):


        for sub_type in self.data_type_config.get_sub_type_objects():
            logger.info("Loading Orthology Data: %s" % sub_type.get_data_provider())
            filepath = sub_type.get_filepath()
            data = JSONFile().get_data(filepath)
            
            logger.info("Finished Loading Orthology Data: %s" % sub_type.get_data_provider())

    
            commit_size = self.data_type_config.get_neo4j_commit_size()
            batch_size = self.data_type_config.get_generator_batch_size()
            
            generators = self.get_generators(data, batch_size)
    
            query_list = [
                [OrthologyETL.query_template, commit_size, "orthology_data_" + sub_type.get_data_provider() + ".csv"],
                [OrthologyETL.matched_algorithm_template, commit_size, "orthology_matched_data_" + sub_type.get_data_provider() + ".csv"],
                [OrthologyETL.unmatched_algorithm_template, commit_size, "orthology_unmatched_data_" + sub_type.get_data_provider() + ".csv"],
                [OrthologyETL.not_called_template, commit_size, "orthology_called_data_" + sub_type.get_data_provider() + ".csv"],
            ]
                
            CSVTransactor.execute_transaction(generators, query_list)

    def get_generators(self, ortho_data, batch_size):

        counter = 0
        matched_data = []
        unmatched_data = []
        ortho_data_list = []
        notcalled_data = []

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
                        "algorithm": matched
                    }
                    matched_data.append(matched_dataset)

                for unmatched in orthoRecord.get('predictionMethodsNotMatched'):
                    unmatched_dataset = {
                        "uuid": ortho_uuid,
                        "algorithm": unmatched
                    }
                    unmatched_data.append(unmatched_dataset)

                for notcalled in orthoRecord.get('predictionMethodsNotCalled'):
                    notcalled_dataset = {
                        "uuid": ortho_uuid,
                        "algorithm": notcalled
                    }
                    notcalled_data.append(notcalled_dataset)

                # Establishes the number of entries to yield (return) at a time.
                if counter == batch_size:
                    yield [ortho_data_list, matched_data, unmatched_data, notcalled_data]
                    ortho_data_list = []
                    matched_data = []
                    unmatched_data = []
                    notcalled_data = []
                    counter = 0

        if counter > 0:
            yield [ortho_data_list, matched_data, unmatched_data, notcalled_data]