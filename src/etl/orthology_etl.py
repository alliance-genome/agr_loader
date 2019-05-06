from itertools import permutations
import logging, uuid
import multiprocessing, ijson
from random import shuffle
import codecs
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
                    orth.isBestScore = apoc.convert.toBoolean(row.isBestScore),
                    orth.isBestRevScore = apoc.convert.toBoolean(row.isBestRevScore),
                    orth.confidence = row.confidence,
                    orth.strictFilter = apoc.convert.toBoolean(row.strictFilter),
                    orth.moderateFilter = apoc.convert.toBoolean(row.moderateFilter)
    
            //Create the Association node to be used for the object/doTerm
            CREATE (oa:Association:OrthologyGeneJoin {primaryKey:row.uuid})
                SET oa.joinType = 'orthologous'
            CREATE (g1)-[a1:ASSOCIATION]->(oa)
            CREATE (oa)-[a2:ASSOCIATION]->(g2) """

    not_matched_algorithm_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (ogj:OrthologyGeneJoin {primaryKey:row.uuid})
            MERGE (oa:OrthoAlgorithm {name:row.algorithm})
            CREATE (ogj)-[:NOT_MATCHED]->(oa) """
    
    matched_algorithm_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (ogj:OrthologyGeneJoin {primaryKey:row.uuid})
            MERGE (oa:OrthoAlgorithm {name:row.algorithm})
            CREATE (ogj)-[:MATCHED]->(oa) """
            
    notcalled_algorithm_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (ogj:OrthologyGeneJoin {primaryKey:row.uuid})
            MERGE (oa:OrthoAlgorithm {name:row.algorithm})
            CREATE (ogj)-[:NOT_CALLED]->(oa) """
            

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        
        sub_types = []
        
        for sub_type in self.data_type_config.get_sub_type_objects():
            sub_types.append(sub_type.get_data_provider())
            
        thread_pool = []
        
        query_tracking_list = multiprocessing.Manager().list()
        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type, sub_types, query_tracking_list))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)
        
        queries = []
        for item in query_tracking_list:
            queries.append(item)
            
        
        algo_queries = []
        
        for item in queries:
            if "algorithm" in item[1]:
                algo_queries.append(item)

        main_list = self.get_randomized_list(sub_types)
            
        for file_set in main_list:
            for pair in file_set:
                for item in queries:
                    if pair[0] + "_" + pair[1] in item[1]:
                        logger.debug("Pair: %s Item: %s" % (pair, item[1]))
                        Neo4jTransactor.execute_query_batch([item])
            
            Neo4jTransactor().wait_for_queues()
            
        Neo4jTransactor.execute_query_batch(algo_queries)
  
    def _process_sub_type(self, sub_type, sub_types, query_tracking_list):
        logger.info("Loading Orthology Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        #data = JSONFile().get_data(filepath)

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()
        
        generators = self.get_generators(filepath, sub_type.get_data_provider(), sub_types, batch_size)

        query_list = []

        for mod_sub_type in sub_types:
            if mod_sub_type != sub_type.get_data_provider():
                query_list.append([OrthologyETL.query_template, "100000", "orthology_data_" + sub_type.get_data_provider() + "_" + mod_sub_type + ".csv"])
 
        query_list.append([OrthologyETL.matched_algorithm_template, commit_size, "orthology_matched_algorithm_data_" + sub_type.get_data_provider() + ".csv"])
        query_list.append([OrthologyETL.not_matched_algorithm_template, commit_size, "orthology_not_matched_algorithm_data_" + sub_type.get_data_provider() + ".csv"])
        query_list.append([OrthologyETL.notcalled_algorithm_template, commit_size, "orthology_notcalled_algorithm_data_" + sub_type.get_data_provider() + ".csv"])
            
        query_and_file_list = self.process_query_params(query_list)
        
        CSVTransactor.save_file_static(generators, query_and_file_list)
        
        for item in query_and_file_list:
            query_tracking_list.append(item)

        logger.info("Finished Loading Orthology Data: %s" % sub_type.get_data_provider())

    def get_randomized_list(self, sub_types):
        pairs = [perm for perm in permutations( sub_types, 2)]
        
        list_o = [] 
        counter = 0;
        while (len(list_o) == 0 or len(list_o) > len(sub_types) * 2) and counter < 10000:
        
            list_o = []
        
            shuffle(pairs)
            for pair in pairs:
                inserted = False
                for item in list_o:
                    found = False
                    for item2 in item:
                        if pair[0] in item2 or pair[1] in item2:
                            found = True
                            break 
                    if not found:
                        item.append(pair)
                        inserted = True
                        break
                if not inserted:
                    list_o.append([pair])
                 
            counter += 1
        
        return list_o


    def get_generators(self, datafile, sub_type, sub_types, batch_size):

        counter = 0

        matched_algorithm_data = []
        unmatched_algorithm_data = []
        notcalled_algorithm_data = []

        list_of_mod_lists = {}

        for mod_sub_type in sub_types:
            if mod_sub_type != sub_type:
                list_of_mod_lists[mod_sub_type] = []

        logger.info("streaming json data from %s ..." % datafile)
        with codecs.open(datafile, 'r', 'utf-8') as f:

            for orthoRecord in ijson.items(f, 'data.item'):
                # Sort out identifiers and prefixes.
                gene1 = ETLHelper.process_identifiers(orthoRecord['gene1'])
                # 'DRSC:'' removed, local ID, functions as display ID.
                gene2 = ETLHelper.process_identifiers(orthoRecord['gene2'])
                # 'DRSC:'' removed, local ID, functions as display ID.

                gene1SpeciesTaxonId = orthoRecord['gene1Species']
                gene2SpeciesTaxonId = orthoRecord['gene2Species']

                # Prefixed according to AGR prefixes.
                gene1AgrPrimaryId = ETLHelper.add_agr_prefix_by_species_taxon(gene1, gene1SpeciesTaxonId)

                # Prefixed according to AGR prefixes.
                gene2AgrPrimaryId = ETLHelper.add_agr_prefix_by_species_taxon(gene2, gene2SpeciesTaxonId)
                gene2DataProvider = ETLHelper.get_MOD_from_taxon(str(gene2SpeciesTaxonId))

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
                    list_of_mod_lists[gene2DataProvider].append(ortho_dataset)

                    for matched in orthoRecord.get('predictionMethodsMatched'):
                        matched_dataset = {
                            "uuid": ortho_uuid,
                            "algorithm": matched
                        }
                        matched_algorithm_data.append(matched_dataset)

                    for unmatched in orthoRecord.get('predictionMethodsNotMatched'):
                        unmatched_dataset = {
                            "uuid": ortho_uuid,
                            "algorithm": unmatched
                        }
                        unmatched_algorithm_data.append(unmatched_dataset)

                    for notcalled in orthoRecord.get('predictionMethodsNotCalled'):
                        notcalled_dataset = {
                            "uuid": ortho_uuid,
                            "algorithm": notcalled
                        }
                        notcalled_algorithm_data.append(notcalled_dataset)

                    # Establishes the number of entries to yield (return) at a time.
                    if counter == batch_size:
                        list_to_yeild = []
                        for mod_sub_type in sub_types:
                            if mod_sub_type != sub_type:
                                list_to_yeild.append(list_of_mod_lists[mod_sub_type])
                        list_to_yeild.append(matched_algorithm_data)
                        list_to_yeild.append(unmatched_algorithm_data)
                        list_to_yeild.append(notcalled_algorithm_data)
                      
                        yield list_to_yeild
                    
                        for mod_sub_type in sub_types:
                            if mod_sub_type != sub_type:
                                list_of_mod_lists[mod_sub_type] = []
                        matched_algorithm_data = []
                        unmatched_algorithm_data = []
                        notcalled_algorithm_data = []
                        counter = 0

            if counter > 0:
                list_to_yeild = []
                for mod_sub_type in sub_types:
                    if mod_sub_type != sub_type:
                        list_to_yeild.append(list_of_mod_lists[mod_sub_type])
                list_to_yeild.append(matched_algorithm_data)
                list_to_yeild.append(unmatched_algorithm_data)
                list_to_yeild.append(notcalled_algorithm_data)
                    
                yield list_to_yeild
