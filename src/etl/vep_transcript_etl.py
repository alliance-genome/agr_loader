"""VEP Transcript ETL"""

import logging
import multiprocessing
import uuid
import re
from etl import ETL
from files import TXTFile

from transactors import CSVTransactor
from transactors import Neo4jTransactor

class VEPTranscriptETL(ETL):
    """VEP Transcript ETL"""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    vep_transcript_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (g:Transcript {gff3ID:row.transcriptId})
                MATCH (a:Variant {hgvsNomenclature:row.hgvsNomenclature})

                CREATE (gc:TranscriptLevelConsequence {primaryKey:row.primaryKey})
                SET gc.transcriptLevelConsequence = row.transcriptLevelConsequence,
                    gc.transcriptId = g.primaryKey,
                    gc.variantId = a.hgvsNomenclature,
                    gc.impact = row.impact,
                    gc.aminoAcidReference = row.aminoAcidReference,
                    gc.aminoAcidVariation = row.aminoAcidVariation,
                    gc.aminoAcidChange = row.aminoAcidChange,
                    gc.cdnaStartPosition = row.cdnaStartPosition,
                    gc.cdnaEndPosition = row.cdnaEndPosition,
                    gc.cdnaRange = row.cdnaRange,
                    gc.cdsStartPosition = row.cdsStartPosition,
                    gc.cdsEndPosition = row.cdsEndPosition,
                    gc.cdsRange = row.cdsRange,
                    gc.proteinStartPosition = row.proteinStartPosition,
                    gc.proteinEndPosition = row.proteinEndPosition,
                    gc.proteinRange = row.proteinRange,
                    gc.codonChange = row.codonChange,
                    gc.codonReference = row.codonReference,
                    gc.codonVariation = row.codonVariation,
                    gc.hgvsProteinNomenclature = row.hgvsProteinNomenclature,  
                    gc.hgvsCodingNomenclature = row.hgvsCodingNomenclature, 
                    gc.hgvsVEPGeneNomenclature = row.hgvsVEPGeneNomenclature            

                CREATE (g)-[ggc:ASSOCIATION {primaryKey:row.primaryKey}]->(gc)
                CREATE (a)-[ga:ASSOCIATION {primaryKey:row.primaryKey}]->(gc)
                CREATE (g)-[gv:ASSOCIATION {primaryKey:row.primaryKey}]->(a)

                MERGE(syn:Synonym:Identifier {primaryKey:row.hgvsVEPGeneNomenclature})
                        SET syn.name = row.hgvsVEPGeneNomenclature
                MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn) 
            
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
            [self.vep_transcript_query_template, commit_size,
             "vep_transcript_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(filepath)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def return_range_split_values(self, column, range_match):
        start = ''
        end = ''
        ranger = ''
        if range_match:
            if "-" in column:
                if column == '-':
                    start = ""
                    end = ""
                    ranger = ""
                else:
                    start = column.split("-")[0]
                    end = column.split("-")[1]
                    ranger = column
            elif "/" in column:
                if column == '/':
                    start = ""
                    end = ""
                    ranger = ""
                else:
                    start = column.split("/")[0]
                    end = column.split("/")[1]
                    ranger = column
        else:
            if column == '-' or column == '/':
                start = ""
                end = ""
                ranger = ""
            else:
                start = column
                end = column
                ranger = column
        return start, end, ranger

    def get_generators(self, filepath):
        """Get Generators"""

        data = TXTFile(filepath).get_data()
        vep_maps = []

        for line in data:
            impact = ''
            hgvs_p = ''
            hgvs_c = ''
            hgvs_g = ''
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
                    if key == 'HGVSp':
                        hgvs_p = value
                    if key == 'HGVSc':
                        hgvs_c = value
                    if key == 'HGVSg':
                        hgvs_g = value
            if columns[3].startswith('Gene:'):
                gene_id = columns[3].lstrip('Gene:')
            else:
                gene_id = columns[3]

            position_is_a_range = re.compile('.*-.*')
            cdna_range_match = re.search(position_is_a_range, columns[7])
            cds_range_match = re.search(position_is_a_range, columns[8])
            protein_range_match = re.search(position_is_a_range, columns[9])

            before_after_change = re.compile(".*/.*")
            amino_acid_range_match = re.search(before_after_change, columns[10])
            codon_range_match = re.search(before_after_change, columns[11])

            cdna_start_position, cdna_end_position, cdna_range = self.return_range_split_values(
                columns[7], cdna_range_match
            )
            cds_start_position, cds_end_position, cds_range = self.return_range_split_values(
                columns[8], cds_range_match
            )
            protein_start_position, protein_end_position, protein_range = self.return_range_split_values(
                columns[9], protein_range_match
            )

            amino_acid_reference, amino_acid_variation, amino_acid_change = self.return_range_split_values(
                columns[10], amino_acid_range_match
            )
            codon_reference, codon_variation, codon_change = self.return_range_split_values(
                columns[11], codon_range_match
            )


            vep_result = {"hgvsNomenclature": columns[0],
                              "transcriptLevelConsequence": columns[6],
                              "primaryKey": str(uuid.uuid4()),
                              "impact": impact,
                              "hgvsProteinNomenclature": hgvs_p,
                              "hgvsCodingNomenclature": hgvs_c,
                              "hgvsVEPGeneNomenclature": hgvs_g,
                              "gene": gene_id,
                              "transcriptId": columns[4],
                              "aminoAcidReference": amino_acid_reference,
                              "aminoAcidVariation": amino_acid_variation,
                              "aminoAcidChange": amino_acid_change,
                              "cdnaStartPosition": cdna_start_position,
                              "cdnaEndPosition": cdna_end_position,
                              "cdnaRange": cdna_range,
                              "cdsStartPosition": cds_start_position,
                              "cdsEndPosition": cds_end_position,
                              "cdsRange": cds_range,
                              "proteinStartPosition":protein_start_position,
                              "proteinEndPosition":protein_end_position,
                              "proteinRange": protein_range,
                              "codonReference": codon_reference,
                              "codonVariation": codon_variation,
                              "codonChange": codon_change
                          }

            vep_maps.append(vep_result)

        yield [vep_maps]
