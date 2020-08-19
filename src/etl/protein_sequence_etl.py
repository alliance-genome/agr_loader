"""Protein Sequence ETL"""

import logging
import multiprocessing
import re
from Bio.Alphabet import IUPAC
from etl import ETL
from etl.helpers import AssemblySequenceHelper
from etl.helpers import Neo4jHelper
from Bio.Seq import Seq
from transactors import CSVTransactor, Neo4jTransactor
from data_manager import DataFileManager
from loader_common import ContextInfo

class ProteinSequenceETL(ETL):
    """ProteinSequence ETL"""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    add_protein_sequences_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
        MATCH (t:Transcript {primaryKey:row.transcriptId})
        
        MERGE (p:ProteinSequence {primaryKey:row.transcriptId})
           ON CREATE SET p.proteinSequence = row.proteinSequence
        
        MERGE (ts:CDSSequence {primaryKey:row.transcriptId})
           ON CREATE SET ts.cdsSequence = row.cdsSequence
           
        MERGE (p)-[pt:ASSOCIATION]-(t)
        MERGE (ts)-[tst:ASSOCIATION]-(t)      
        
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

        self.logger.info("Starting Protein Sequence Load")

        query_template_list = [
            [self.add_protein_sequences_query_template, "10000",
             "protein_sequence.csv"]
        ]

        generators = self.get_generators()

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        self.logger.info("Finished Protein Sequence Load")


    def translate_protein(self, cds_sequence, strand):

        self.logger.info(cds_sequence)

        # if the strand of the transcript is '-', we have to reverse complement the sequence before translating.
        if strand == '-':
            reverse_sequence = Seq(cds_sequence).reverse_complement()
            protein_sequence = reverse_sequence.translate(table='Standard', stop_symbol='*', to_stop=False, cds=False, gap=None)
            # if phase == '1':
            #     # remove the first base pair when phase is 1 to start translation appropriately.
            #     reverse_sequence = cds_sequence.reverse_complement()[1:]
            #     protein_sequence = reverse_sequence.translate(table=1)
            # elif phase == '2':
            #     # remove the first two base pairs when phase is 1 to start translation appropriately.
            #     reverse_sequence = cds_sequence.reverse_complement()[2:]
            #     protein_sequence = reverse_sequence.translate(table=1)
        else:
            protein_sequence = Seq(cds_sequence).translate(table='Standard', stop_symbol='*', to_stop=False, cds=False, gap=None)

        # else:
        #     coding_dna = Seq(cds_sequence)
        #     protein_sequence = coding_dna.translate(table=1)

        self.logger.info(protein_sequence)

        return protein_sequence


    def get_generators(self):
        """Get Generators"""

        self.logger.info("reached sequence retrieval retrieval")

        context_info = ContextInfo()
        data_manager = DataFileManager(context_info.config_file_location)

        transcript_data = []

        fetch_transcript_query = """

            MATCH (gl:GenomicLocation)-[glt:ASSOCIATION]-(t:Transcript)-[tt:TRANSCRIPT_TYPE]-(so:SOTerm)
            WHERE so.name = 'mRNA'
            RETURN t.primaryKey as transcriptId,
                   gl.assembly as transcriptAssembly,
                   gl.chromosome as transcriptChromosome,
                   gl.strand as transcriptStrand

        """

        fetch_cds_per_transcript_query = """

            MATCH (gl:GenomicLocation)-[gle:ASSOCIATION]-(e:CDS)-[et:CDS]-(t:Transcript)
            WHERE t.primaryKey = {parameter}
            AND t.dataProvider in ['FB', 'WB', 'ZFIN', 'RGD', 'MGI']
            RETURN gl.end AS CDSEndPosition, 
                   gl.start AS CDSStartPosition, 
                   t.primaryKey as transcriptPrimaryKey,
                   t.dataProvider as dataProvider,
                   gl.phase as CDSPhase
                   order by gl.start 
        """
        return_set_t = Neo4jHelper().run_single_query(fetch_transcript_query)

        for record in return_set_t:

            transcript_id = record['transcriptId']
            transcript_assembly = record['transcriptAssembly']
            transcript_chromosome = record['transcriptChromosome']
            transcript_strand = record['transcriptStrand']

            assemblies = {}
            return_set_cds = Neo4jHelper().run_single_parameter_query(fetch_cds_per_transcript_query,
                                                                      transcript_id)
            full_cds_sequence = ''

            for cds_record in return_set_cds:
                assemblies[transcript_assembly] = AssemblySequenceHelper(transcript_assembly, data_manager)
                start_position = cds_record["CDSStartPosition"]
                end_position = cds_record["CDSEndPosition"]
                # phase = cds_record["CDSPhase"]
                transcript_id = cds_record["transcriptPrimaryKey"]
                cds_sequence = assemblies[transcript_assembly].get_sequence(transcript_chromosome,
                                                                           start_position,
                                                                           end_position)

                full_cds_sequence += cds_sequence

            protein_sequence = self.translate_protein(full_cds_sequence, transcript_strand)

            data = { "transcriptId": transcript_id,
                     "CDSSequence": full_cds_sequence,
                     "proteinSequence": protein_sequence
            }
            transcript_data.append(data)


        yield [transcript_data]