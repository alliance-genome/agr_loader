"""Protein Sequence ETL"""

import logging
import multiprocessing
from etl import ETL
from etl.helpers import AssemblySequenceHelper
from etl.helpers import Neo4jHelper
from Bio.Seq import Seq
from transactors import CSVTransactor, Neo4jTransactor
from data_manager import DataFileManager
from loader_common import ContextInfo
from Bio import BiopythonWarning
import warnings
with warnings.catch_warnings():
    warnings.simplefilter('ignore', BiopythonWarning)

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

        batch_size = 10000
        generators = self.get_generators(batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        self.logger.info("Finished Protein Sequence Load")


    def translate_protein(self, cds_sequence, strand):

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


        return protein_sequence


    def get_generators(self, batch_size):
        """Get Generators"""
        self.logger.debug("reached sequence retrieval retrieval")

        transcript_data = []
        assemblies = {}

        fetch_transcript_query = """

                   MATCH (gl:GenomicLocation)-[gle:ASSOCIATION]-(t:Transcript)-[tv:ASSOCIATION]-(v:Variant)
                   RETURN t.primaryKey as transcriptPrimaryKey,
                          t.dataProvider as dataProvider,
                          gl.phase as transcriptPhase,
                          gl.assembly as transcriptAssembly,
                          gl.chromosome as transcriptChromosome,
                          gl.strand as transcriptStrand
                          order by transcriptPrimaryKey 
               """


        fetch_cds_transcript_query = """

            MATCH (gl:GenomicLocation)-[gle:ASSOCIATION]-(e:CDS)-[et:CDS]-(t:Transcript)-[tv:ASSOCIATION]-(v:Variant)
            RETURN gl.end AS CDSEndPosition, 
                   gl.start AS CDSStartPosition, 
                   t.primaryKey as transcriptPrimaryKey,
                   t.dataProvider as dataProvider,
                   gl.phase as CDSPhase,
                   gl.assembly as CDSAssembly,
                   gl.chromosome as CDSChromosome,
                   gl.strand as CDSStrand
                   order by transcriptPrimaryKey, CDSStartPosition 
        """

        # get all transcripts to iterate through.
        return_set_t = Neo4jHelper().run_single_query(fetch_transcript_query)

        # get all CDS coordinates for all transcripts.
        return_set_cds = Neo4jHelper().run_single_query(fetch_cds_transcript_query)
        returned_cds = []

        # Process the query results into a list that can be cycled through many times to pull
        # CDS group for each transcript.
        for setcds in return_set_cds:
            cds = {
                "CDSChromosome": setcds["CDSChromosome"],
                "CDSStartPosition": setcds["CDSStartPosition"],
                "CDSEndPosition": setcds["CDSEndPosition"],
                "CDSAssembly": setcds["CDSAssembly"],
                "transcriptPrimaryKey": setcds["transcriptPrimaryKey"]
            }
            returned_cds.append(cds)
        counter = 0

        for transcript_record in return_set_t:
            counter =+ 1
            context_info = ContextInfo()
            data_manager = DataFileManager(context_info.config_file_location)
            assembly = transcript_record['transcriptAssembly']
            assemblies[assembly] = AssemblySequenceHelper(assembly, data_manager)
            transcript_id = transcript_record['transcriptPrimaryKey']

            full_cds_sequence = ''
            strand = ''

            for cds_record in returned_cds:
                if cds_record['transcriptPrimaryKey'] == transcript_id:
                    cds_sequence = assemblies[assembly].get_sequence(cds_record["CDSChromosome"],
                                                                              cds_record["CDSStartPosition"],
                                                                              cds_record["CDSEndPosition"])
                    full_cds_sequence += cds_sequence
                    strand = transcript_record['transcriptStrand']

            protein_sequence = self.translate_protein(full_cds_sequence, strand)


            data = { "transcriptId": transcript_id,
                     "CDSSequence": full_cds_sequence,
                     "proteinSequence": protein_sequence
            }
            transcript_data.append(data)
            if counter > batch_size:
                self.logger.info("finished batch " + counter)
                yield [transcript_data]
                transcript_data = []
                counter = 0 
        if counter > 0:
            yield [transcript_data]