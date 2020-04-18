import logging
import multiprocessing
import uuid
import re
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
                    gc.impact = row.impact,
                    gc.amino_acid_reference = row.amino_acid_reference,
                    gc.amino_acid_variation = row.amino_acid_variation,
                    gc.amino_acid_change = row.amino_acid_change,
                    gc.cdna_start_position = row.cdna_start_position,
                    gc.cdna_end_position = row.cdna_end_position,
                    gc.cdna_range = row.cdna_range,
                    gc.cds_start_position = row.cds_start_position,
                    gc.cds_end_position = row.cds_end_position,
                    gc.cds_range = row.cds_range,
                    gc.protein_start_position = row.protein_start_position,
                    gc.protein_end_position = row.protein_end_position,
                    gc.protein_range = row.protein_range,                    

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
        amino_acid_reference = ''
        amino_acid_variation = ''

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
                if "/" in columns[10]:
                    amino_acid_reference = columns[10].split("/")[0]
                    amino_acid_variation = columns[10].split("/")[1]
                    amino_acid_change = columns[10]
                else:
                    amino_acid_change = columns[10]

                if amino_acid_change == '-':
                    amino_acid_change = ""


                position_is_a_range = re.compile('[0-9]*-[0-9]*')
                cdna_range_match = re.search(position_is_a_range, columns[7])
                cds_range_match = re.search(position_is_a_range, columns[8])
                protein_range_match = re.search(position_is_a_range, columns[9])

                if cdna_range_match:
                    cdna_start_position = columns[7].split("-")[0]
                    cdna_end_position = columns[7].split("-")[1]
                    cdna_range = columns[7]
                else:
                    if columns[7] == '-':
                        cdna_start_position = ""
                        cdna_end_position = ""
                        cdna_range = ""
                    else:
                        cdna_start_position = columns[7]
                        cdna_end_position = columns[7]
                        cdna_range = columns[7]

                if cds_range_match:
                    cds_start_position = columns[8].split("-")[0]
                    cds_end_position = columns[8].split("-")[1]
                    cds_range = columns[8]
                else:
                    if columns[8] == '-':
                        cds_start_position = ""
                        cds_end_position = ""
                        cds_range = ""
                    else:
                        cds_start_position = columns[8]
                        cds_end_position = columns[8]
                        cds_range = columns[8]

                if protein_range_match:
                    protein_start_position = columns[9].split("-")[0]
                    protein_end_position = columns[9].split("-")[1]
                    protein_range = columns[9]
                else:
                    if columns[9] == '-':
                        protein_start_position = ""
                        protein_end_position = ""
                        protein_range = ""
                    else:
                        protein_start_position = columns[8]
                        protein_end_position = columns[8]
                        protein_range = columns[8]

                vep_result = {"hgvsNomenclature": columns[0],
                              "transcriptLevelConsequence": columns[6],
                              "primaryKey": str(uuid.uuid4()),
                              "impact": impact,
                              "gene": geneId,
                              "transcriptId": columns[4],
                              "amino_acid_reference": amino_acid_reference,
                              "amino_acid_variation": amino_acid_variation,
                              "amino_acid_change": amino_acid_change,
                              "cdna_start_position": cdna_start_position,
                              "cdna_end_position": cdna_end_position,
                              "cdna_range": cdna_range,
                              "cds_start_position": cds_start_position,
                              "cds_end_position": cds_end_position,
                              "cds_range": cds_range,
                              "protein_start_position":protein_start_position,
                              "protein_end_position":protein_end_position,
                              "protein_range": protein_range}

                if columns[4] == 'FBtr0079106':
                    logger.info(vep_result)
                vep_maps.append(vep_result)

        yield [vep_maps]

