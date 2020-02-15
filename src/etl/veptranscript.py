import logging
import multiprocessing
import uuid
from etl import ETL
from files import TXTFile

from transactors import CSVTransactor
from transactors import Neo4jTransactor

logger = logging.getLogger(__name__)


class VEPTRANSCRIPTETL(ETL):
    vep_transcript_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (g:Transcript {gff3ID:row.transcriptId})
                MATCH (a:Variant {primaryKey:row.hgvsNomenclature})

                CREATE (gc:TranscriptLevelConsequence {primaryKey:row.primaryKey})
                SET gc.transcriptLevelConsequence = row.transcriptLevelConsequence,
                    gc.transcriptId = g.primaryKey,
                    gc.variantId = a.hgvsNomenclature,
                    gc.impact = row.impact

                CREATE (g)-[ggc:ASSOCIATION {primaryKey:row.primaryKey}]->(gc)
                CREATE (a)-[ga:ASSOCIATION {primaryKey:row.primaryKey}]->(gc)

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
            [VEPTRANSCRIPTETL.vep_transcript_query_template, commit_size, "vep_transcript_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(filepath)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, filepath):

        data = TXTFile(filepath).get_data()
        vep_maps = []
        impact = ''

        for line in data:
            columns = line.split()
            if columns[0].startswith('#'):
                continue
            else:
                notes = columns[13]
                kvpairs = notes.split(";")
                if kvpairs is not None:
                    for pair in kvpairs:
                        key = pair.split("=")[0]
                        value = pair.split("=")[1]
                        if key == 'IMPACT':
                            impact = value
                if columns[3].startswith('Gene:'):
                    geneId = columns[3].lstrip('Gene:')
                elif columns[3].startswith('RGD:'):
                    geneId = columns[3].lstrip('RGD:')
                else:
                    geneId = columns[3]

                vep_result = {"hgvsNomenclature": columns[0],
                              "transcriptLevelConsequence": columns[6],
                              "primaryKey": str(uuid.uuid4()),
                              "impact": impact,
                              "gene": geneId,
                              "transcriptId": columns[4]}
                if columns[4] == 'FBtr0079106':
                    logger.info(vep_result)
                vep_maps.append(vep_result)

        yield [vep_maps]

