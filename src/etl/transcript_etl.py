import logging
import multiprocessing
import uuid
from etl import ETL
import re

from transactors import CSVTransactor
from transactors import Neo4jTransactor

logger = logging.getLogger(__name__)


class TranscriptETL(ETL):


    exon_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (g:Transcript {gff3ID: row.parentId})
                MATCH (so:SOTerm {name: row.featureType})

                MERGE (t:Exon {primaryKey:row.gff3ID})
                    ON CREATE SET t.gff3ID = row.gff3ID,
                        t.dataProvider = row.dataProvider,
                        t.name = row.name        

               MERGE (t)<-[tso:TYPE]-(so)
               MERGE (g)<-[gt:EXON]-(t)
                """


    exon_genomic_locations_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Exon {primaryKey: row.gff3ID})
            MATCH (chrm:Chromosome {primaryKey: row.chromosomeNumber})

            MERGE (a:Assembly {primaryKey: row.assembly})

            CREATE (o)-[ochrm:LOCATED_ON]->(chrm)                

            CREATE (gchrm:GenomicLocation {primaryKey: row.genomicLocationUUID})
              SET gchrm.start = apoc.number.parseInt(row.start),
                gchrm.end = apoc.number.parseInt(row.end),
                gchrm.assembly = row.assembly,
                gchrm.strand = row.strand,
                gchrm.chromosome = row.chromosomeNumber

            CREATE (o)-[of:ASSOCIATION]->(gchrm)
            CREATE (gchrm)-[ofc:ASSOCIATION]->(chrm)
            CREATE (a)-[ao:ASSOCIATION]->(o)

        """

    tscript_alternate_id_query_template = """
            USING PERIODIC COMMIT %s
                LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            
                MATCH (g:Gene {primaryKey:row.curie})
                  SET g.gff3ID = row.gff3ID
    
    """

    tscript_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (g:Gene {gff3ID: row.parentId})
                MATCH (so:SOTerm {name: row.featureType})

                MERGE (t:Transcript {primaryKey:row.curie})
                    ON CREATE SET t.gff3ID = row.gff3ID,
                        t.dataProvider = row.dataProvider,
                        t.name = row.name        
                
               MERGE (t)<-[tso:TRANSCRIPT_TYPE]-(so)
               MERGE (g)<-[gt:TRANSCRIPT]-(t)
                """

    chromosomes_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MERGE (chrm:Chromosome {primaryKey: row.chromosomeNumber}) """


    genomic_locations_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Transcript {primaryKey: row.curie})
            MATCH (chrm:Chromosome {primaryKey: row.chromosomeNumber})
            
            MERGE (a:Assembly {primaryKey: row.assembly})
            
            CREATE (o)-[ochrm:LOCATED_ON]->(chrm)                

            CREATE (gchrm:GenomicLocation {primaryKey: row.genomicLocationUUID})
              SET gchrm.start = apoc.number.parseInt(row.start),
                gchrm.end = apoc.number.parseInt(row.end),
                gchrm.assembly = row.assembly,
                gchrm.strand = row.strand,
                gchrm.chromosome = row.chromosomeNumber

            CREATE (o)-[of:ASSOCIATION]->(gchrm)
            CREATE (gchrm)-[ofc:ASSOCIATION]->(chrm)
            CREATE (a)-[ao:ASSOCIATION]->(o)

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
        batch_size = self.data_type_config.get_generator_batch_size()
        filepath = sub_type.get_filepath()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [TranscriptETL.tscript_alternate_id_query_template, commit_size, "transcript_gff3ID_data_" + sub_type.get_data_provider() + ".csv"],
            [TranscriptETL.tscript_query_template, commit_size, "transcript_data_" + sub_type.get_data_provider() + ".csv"],
            [TranscriptETL.chromosomes_template, commit_size, "transcript_data_chromosome_" + sub_type.get_data_provider() + ".csv"],
            [TranscriptETL.genomic_locations_template, commit_size, "transcript_genomic_locations_" + sub_type.get_data_provider() + ".csv"],
            [TranscriptETL.exon_query_template, commit_size, "exon_data_" + sub_type.get_data_provider() + ".csv"],
            [TranscriptETL.exon_genomic_locations_template, commit_size, "exon_genomic_location_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(filepath, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, filepath, batch_size):

        with open(filepath) as f:
            tscriptMaps = []
            geneMaps =[]
            exonMaps = []
            counter = 0
            dataProvider = ''
            assembly = ''
            for line in f:
                counter = counter + 1
                transcriptMap = {}
                geneMap = {}
                exonMap = {}
                curie = ''
                parent = ''
                gff3ID = ''
                tscriptTypes = ['mRNA','miRNA','ncRNA','rRNA','snRNA','snoRNA','tRNA','pre_miRNA','lnc_RNA']
                possibleTypes = ['gene','mRNA','miRNA','ncRNA','rRNA','snRNA','snoRNA','tRNA','pre_miRNA','lnc_RNA','exon']
                gene_id = ''

                if line.startswith('#!'):
                    headerColumns = line.split()
                    if line.startswith('#!assembly'):
                        assembly = headerColumns[1]

                    elif line.startswith('#!data-source '):
                        dataProvider = headerColumns[1]
                        if dataProvider == 'FlyBase':
                            dataProvider = 'FB'
                        if dataProvider == 'WormBase':
                            dataProvider = 'WB'
                        logger.info("datasource " + headerColumns[1])
                elif line.startswith('#'):
                    continue
                else:
                    columns = re.split(r'\t', line)
                    featureTypeName = columns[2].strip()
                    if featureTypeName in possibleTypes:
                        column8 = columns[8]
                        notes = "_".join(column8.split())
                        kvpairs = re.split(';', notes)
                        if kvpairs is not None:
                            for pair in kvpairs:
                                if "=" in pair:
                                    key = pair.split("=")[0]
                                    value = pair.split("=")[1]
                                    if key == 'ID':
                                        gff3ID = value
                                    if key == 'gene_id':
                                        gene_id = value
                                    if key == 'Parent':
                                        parent = value
                                    if key == 'Name':
                                        name = value
                                    #if key == 'Alias':
                                       #aliases = value.split(',')
                                #       transcriptMap.update({'aliases' : aliases})
                                #       if key == 'SecondaryIds':
                                #           secIds = value.split(',')
                                #           transcriptMap.update({'secIds' : secIds})
                                    if key == 'curie':
                                        curie = value
                                # gene: curie = RGD:1309770 ID=RGD:1309770

                            # gene: ID=MGI_C57BL6J_3588256 curie=MGI:3588256
                            # transcript: ID=MGI_C57BL6J_3588256_transcript_1 curie=NCBI_Gene:NM_001033977.2 Parent=MGI_C57BL6J_3588256

                                if self.testObject.using_test_data() is True:

                                    is_it_test_entry = self.testObject.check_for_test_id_entry(curie)


                                    if is_it_test_entry is False:
                                        is_it_test_entry = self.testObject.check_for_test_id_entry(parent)

                                        if is_it_test_entry is False:
                                            is_it_test_entry = self.testObject.check_for_test_id_entry(gene_id)

                                            if is_it_test_entry is True:
                                                counter = counter - 1
                                            continue
                        if featureTypeName in tscriptTypes:
                            transcriptMap.update({'curie' : curie})
                            transcriptMap.update({'parentId': parent})
                            transcriptMap.update({'gff3ID': gff3ID})
                            transcriptMap.update({'genomicLocationUUID': str(uuid.uuid4())})
                            transcriptMap.update({'chromosomeNumber': columns[0]})
                            transcriptMap.update({'featureType': featureTypeName})
                            transcriptMap.update({'start':columns[3]})
                            transcriptMap.update({'end':columns[4]})
                            transcriptMap.update({'assembly': assembly})
                            transcriptMap.update({'dataProvider': dataProvider})
                            transcriptMap.update({'name': name})
                            if assembly is None:
                                assembly = 'assembly_unlabeled_in_gff3_header'
                                transcriptMap.update({'assembly':assembly})
                            tscriptMaps.append(transcriptMap)
                        elif featureTypeName == 'gene':
                            geneMap.update({'curie': curie})
                            geneMap.update({'parentId': parent})
                            geneMap.update({'gff3ID': gff3ID})
                            geneMaps.append(geneMap)
                        elif featureTypeName == 'exon':
                            exonMap.update({'parentId': parent})
                            exonMap.update({'gff3ID': str(uuid.uuid4())})
                            exonMap.update({'genomicLocationUUID': str(uuid.uuid4())})
                            exonMap.update({'chromosomeNumber': columns[0]})
                            exonMap.update({'featureType': featureTypeName})
                            exonMap.update({'start':columns[3]})
                            exonMap.update({'end':columns[4]})
                            exonMap.update({'assembly': assembly})
                            exonMap.update({'dataProvider': dataProvider})
                            exonMap.update({'name': name})
                            if assembly is None:
                                assembly = 'assembly_unlabeled_in_gff3_header'
                                exonMap.update({'assembly':assembly})
                            exonMaps.append(exonMap)
                        else:
                            continue


                if counter == batch_size:
                    counter = 0
                    yield [geneMaps, tscriptMaps, tscriptMaps, tscriptMaps, exonMaps, exonMaps]
                    tscriptMaps = []
                    geneMaps = []
                    exonMaps = []


            if counter > 0:
                yield [geneMaps, tscriptMaps, tscriptMaps, tscriptMaps, exonMaps, exonMaps]

