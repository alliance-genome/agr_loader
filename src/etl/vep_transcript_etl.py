"""VEP Transcript ETL."""

import logging
import multiprocessing
import uuid
import re
from etl import ETL
from files import TXTFile

from transactors import CSVTransactor
from transactors import Neo4jTransactor


class VEPTranscriptETL(ETL):
    """VEP Transcript ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    vep_transcript_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (g:Transcript {primaryKey:row.transcriptId})
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
                    gc.hgvsVEPGeneNomenclature = row.hgvsVEPGeneNomenclature,
                    gc.polyphenPrediction = row.polyphenPrediction,
                    gc.polyphenScore = row.polyphenScore,
                    gc.siftPrediction = row.siftPrediction,
                    gc.siftScore = row.siftScore

                CREATE (g)-[ggc:ASSOCIATION {primaryKey:row.primaryKey}]->(gc)
                CREATE (a)-[ga:ASSOCIATION {primaryKey:row.primaryKey}]->(gc)
                CREATE (g)-[gv:ASSOCIATION {primaryKey:row.primaryKey}]->(a)
                
                CREATE (p:VariantProteinSequence {primaryKey:row.variantProteinSequenceKey})
                  SET p.proteinSequence = row.variantProteinSequence
                  SET p.variantId = row.hgvsNomenclature
                  SET p.transcriptId = row.transcriptId
                  
                
                CREATE (a)-[ps:PROTEIN_SEQUENCE]->(p)

                MERGE(syn:Synonym:Identifier {primaryKey:row.hgvsVEPGeneNomenclature})
                        SET syn.name = row.hgvsVEPGeneNomenclature
                MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn)

            """

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(
                target=self._process_sub_type, args=(sub_type,))
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
        self.error_messages("VEPTran-{}: ".format(sub_type.get_data_provider()))

    def return_range_split_values(self, column):
        """Get range vaues."""
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
            start = column
            end = column
            ranger = column
        return start, end, ranger

    def get_generators(self, filepath):
        """Get Generators."""
        data = TXTFile(filepath).get_data()
        vep_maps = []

        prot_func_regex = re.compile(r'^([^\(]+)\(([\d\.]+)\)')

        for line in data:
            impact = ''
            hgvs_p = ''
            hgvs_c = ''
            hgvs_g = ''
            pph_prediction = ''
            pph_score = ''
            sift_prediction = ''
            sift_score = ''
            variant_protein_sequnece = ''
            transcript_wt_sequence = ''

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
                    elif key == 'HGVSp':
                        hgvs_p = value
                    elif key == 'HGVSc':
                        hgvs_c = value
                    elif key == 'HGVSg':
                        hgvs_g = value
                    elif key == 'PolyPhen':
                        m = prot_func_regex.match(value)
                        pph_prediction = m.group(1)
                        pph_score = m.group(2)
                    elif key == 'SIFT':
                        m = prot_func_regex.match(value)
                        sift_prediction = m.group(1)
                        sift_score = m.group(2)
                    elif key == 'VarSeq':
                        variant_protein_sequnece = value
                    elif key == 'WtSeq':
                        transcript_wt_sequence = value


            if columns[3].startswith('Gene:'):
                gene_id = columns[3].lstrip('Gene:')
            else:
                gene_id = columns[3]

            cdna_start_position, cdna_end_position, cdna_range = self.return_range_split_values(
                columns[7]
            )
            cds_start_position, cds_end_position, cds_range = self.return_range_split_values(
                columns[8]
            )
            protein_start_position, protein_end_position, protein_range = self.return_range_split_values(
                columns[9]
            )

            amino_acid_reference, amino_acid_variation, amino_acid_change = self.return_range_split_values(
                columns[10]
            )
            codon_reference, codon_variation, codon_change = self.return_range_split_values(
                columns[11]
            )

            transcript_id = columns[4]
            hgvsNomenclature = columns[0]
            vep_result = {"hgvsNomenclature": hgvsNomenclature,
                          "transcriptLevelConsequence": columns[6],
                          "primaryKey": str(uuid.uuid4()),
                          "impact": impact,
                          "hgvsProteinNomenclature": hgvs_p,
                          "hgvsCodingNomenclature": hgvs_c,
                          "hgvsVEPGeneNomenclature": hgvs_g,
                          "gene": gene_id,
                          "transcriptId": transcript_id,
                          "aminoAcidReference": amino_acid_reference,
                          "aminoAcidVariation": amino_acid_variation,
                          "aminoAcidChange": amino_acid_change,
                          "cdnaStartPosition": cdna_start_position,
                          "cdnaEndPosition": cdna_end_position,
                          "cdnaRange": cdna_range,
                          "cdsStartPosition": cds_start_position,
                          "cdsEndPosition": cds_end_position,
                          "cdsRange": cds_range,
                          "proteinStartPosition": protein_start_position,
                          "proteinEndPosition": protein_end_position,
                          "proteinRange": protein_range,
                          "codonReference": codon_reference,
                          "codonVariation": codon_variation,
                          "codonChange": codon_change,
                          "polyphenPrediction": pph_prediction,
                          "polyphenScore": pph_score,
                          "siftPrediction": sift_prediction,
                          "siftScore": sift_score,
                          "variantProteinSequence": variant_protein_sequnece,
                          "variantProteinSequenceKey": transcript_id+hgvsNomenclature,
                          "transcriptWtSequence": transcript_wt_sequence,
                          "transcriptProteinSequenceKey": transcript_id+"Protein"
                          }

            vep_maps.append(vep_result)

        yield [vep_maps]
