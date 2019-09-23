import logging
import multiprocessing
import uuid
from etl import ETL
from files import TXTFile

from transactors import CSVTransactor
from transactors import Neo4jTransactor

logger = logging.getLogger(__name__)


class VEPETL(ETL):

    vep_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (g:Gene {modLocalId:row.geneId})
                MATCH (a:Variant {primaryKey: row.hgvsNomenclature})
                
                MERGE (gc:GeneLevelConsequence {primaryKey:row.primaryKey})
                ON CREATE SET gc.geneLevelConsequence = row.geneLevelConsequence,
                    gc.geneId = g.primaryKey,
                    gc.variantId = a.hgvsNomenclature
                
                MERGE (g)-[ggc:ASSOCIATION {primaryKey:row.primaryKey}]-(gc)
                MERGE (a)-[ga:ASSOCATION {primaryKey:row.primaryKey}]-(gc)
                
                """


    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):
        logger.info("Loading VEP Data: %s" % sub_type.get_data_provider())
        commit_size = self.data_type_config.get_neo4j_commit_size()
        filepath = sub_type.get_filepath()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [VEPETL.vep_query_template, commit_size, "vep_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(filepath)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, filepath):

        data = TXTFile(filepath).get_data()
        vep_maps = []

        for line in data:
            columns = line.split()
            if columns[0].startswith('#'):
                continue
            else:
                notes = columns[7]
                notes.split(";")
                vep_result = {"hgvsNomenclature":columns[0],
                       "geneLevelConsequence": columns[6],
                       "primaryKey": str(uuid.uuid4()),
                              "geneId": columns[3]}
                vep_maps.append(vep_result)

        yield [vep_maps]
