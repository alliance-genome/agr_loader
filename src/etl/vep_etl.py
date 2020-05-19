'''VEP ETL'''

import logging
import multiprocessing
from etl import ETL
from files import TXTFile

from transactors import CSVTransactor
from transactors import Neo4jTransactor


class VEPETL(ETL):
    '''VEP ETL'''


    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    vep_gene_query_template = """
               USING PERIODIC COMMIT %s
               LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                   MATCH (g:Gene {modLocalId:row.geneId})
                   MATCH (a:Variant {primaryKey:row.hgvsNomenclature})

                   MERGE (gc:GeneLevelConsequence {primaryKey:row.primaryKey})
                   ON CREATE SET gc.geneLevelConsequence = row.geneLevelConsequence,
                       gc.geneId = g.primaryKey,
                       gc.variantId = a.hgvsNomenclature,
                       gc.impact = row.impact

                   MERGE (g)-[ggc:ASSOCIATION {primaryKey:row.primaryKey}]->(gc)
                   MERGE (a)-[ga:ASSOCIATION {primaryKey:row.primaryKey}]->(gc)

                   """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):
        self.logger.info("Loading VEP Data: %s", sub_type.get_data_provider())
        commit_size = self.data_type_config.get_neo4j_commit_size()
        filepath = sub_type.get_filepath()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.vep_gene_query_template, commit_size,
             "vep_gene_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(filepath)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    @staticmethod
    def get_generators(filepath):
        '''Get Generators'''

        data = TXTFile(filepath).get_data()
        vep_maps = []
        impact = ''

        for line in data:
            columns = line.split()
            if columns[0].startswith('#'):
                continue

            notes = columns[13]
            kvpairs = notes.split(";")
            if kvpairs is not None:
                for pair in kvpairs:
                    key = pair.split("=")[0]
                    value = pair.split("=")[1]
                    if key == 'IMPACT':
                        impact = value
            if columns[3].startswith('Gene:'):
                gene_id = columns[3].lstrip('Gene:')
            elif columns[3].startswith('RGD:'):
                gene_id = columns[3].lstrip('RGD:')
            else:
                gene_id = columns[3]

            vep_result = {"hgvsNomenclature": columns[0],
                          "geneLevelConsequence": columns[6],
                          "primaryKey": columns[0] + columns[6] + impact + gene_id,
                          "impact": impact,
                          "geneId":gene_id}
            vep_maps.append(vep_result)

        yield [vep_maps]
