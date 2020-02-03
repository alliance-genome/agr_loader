import logging
import multiprocessing
import uuid
from etl import ETL
from files import TXTFile

from transactors import CSVTransactor
from transactors import Neo4jTransactor

logger = logging.getLogger(__name__)


class TranscriptETL(ETL):
    tscript_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (a:Gene {primaryKey:row.parent})
                MATCH (so:SOTerm {name:row.featureTypeName})
                
                CREATE (t:Transcript {primaryKey:row.curie})
                SET t.gff3ID = row.ID
                
                MERGE (t)-[tso:TRANSCRIPT_TYPE]-(so)

                """

    chromosomes_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MERGE (chrm:Chromosome {primaryKey: row.chromosomeName}) """


    genomic_locations_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Transcript {primaryKey:row.curie})
            MATCH (chrm:Chromosome {primaryKey:row.chromosomeName})

            MERGE (o)-[ochrm:LOCATED_ON]->(chrm)            
            MERGE (a:Assembly {primaryKey:row.assembly})

            MERGE (gchrm:GenomicLocation {primaryKey:row.uuid})
            ON CREATE SET gchrm.start = apoc.number.parseInt(row.start),
                gchrm.end = apoc.number.parseInt(row.end),
                gchrm.assembly = row.assembly,
                gchrm.strand = row.strand,
                gchrm.chromosome = row.chromosome

            MERGE (o)-[of:ASSOCIATION]-(gchrm)
            MERGE (gchrm)-[ofc:ASSOCIATION]-(chrm)

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
        logger.info("Loading Transcript Data: %s" % sub_type.get_data_provider())
        commit_size = self.data_type_config.get_neo4j_commit_size()
        filepath = sub_type.get_filepath()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [TranscriptETL.tscript_query_template, commit_size, "transcript_data_" + sub_type.get_data_provider() + ".csv"],
            [TranscriptETL.chromosomes_template, commit_size, "transcript_data_chromosome_" + sub_type.get_data_provider() + ".csv"],
            [TranscriptETL.genomic_locations_template, commit_size, "transcript_genomic_locations_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(filepath)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, filepath):

        data = TXTFile(filepath).get_data()
        tscript_maps = []
        impact = ''
        geneId = ''

        for line in data:
            transcriptMap = {}
            columns = line.split()
            if columns[0].startsWith('#!'):
                headerText = columns[0].split(" ")
                if headerText[0] == 'assmebly':
                    transcriptMap.update({'assembly':headerText[1]})
            elif columns[0].startswith('#'):
                continue
            else:
                featureTypeName = columns[3]
                logger.info("featureTypeName: "+featureTypeName)
                notes = columns[9]
                kvpairs = notes.split(";")
                transcriptMap.update({'genomicLocationUUID': str(uuid.uuid4())})
                transcriptMap.update({'chromosomeNumber':columns[1]})
                transcriptMap.update({'featureType':featureTypeName})
                if kvpairs is not None:
                    for pair in kvpairs:
                        key = pair.split("=")[0]
                        value = pair.split("=")[1]
                        if key == 'ID':
                            ID = value
                            transcriptMap.update({'gff3ID' : ID})
                        if key == 'Parent':
                            parent = value
                            transcriptMap.update({'parent' : parent})
                        if key == 'Alias':
                            aliases = value.split(',')
                            transcriptMap.update({'aliases' : aliases})
                        if key == 'SecondaryIds':
                            secIds = value.split(',')
                            transcriptMap.update({'secIds' : secIds})
                        if key == 'curie':
                            transcriptMap.update({'curie' : value})

                transcriptMap.update({'start':columns[4]})
                transcriptMap.update({'end':columns[5]})

                tscript_maps.append(transcriptMap)

        yield [tscript_maps]
