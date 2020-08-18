"""Transcript ETL"""

import re
import logging
import multiprocessing
import uuid

from etl import ETL
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class TranscriptETL(ETL):
    """Transcript ETL"""


    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later


    exon_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (g:Transcript {gff3ID: row.parentId})
                MATCH (so:SOTerm {name: row.featureType})

                MERGE (t:Exon {primaryKey:row.gff3ID})
                    ON CREATE SET t.gff3ID = row.gff3ID,
                        t.dataProvider = row.dataProvider,
                        t.name = row.name,
                        t.synonym = row.synonym        

               CREATE (t)<-[tso:TYPE]-(so)
               CREATE (g)<-[gt:EXON]-(t)"""

    exon_genomic_locations_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Exon {primaryKey: row.gff3ID})
            MATCH (chrm:Chromosome {primaryKey: row.chromosomeNumber})
            MATCH (a:Assembly {primaryKey: row.assembly})

            CREATE (o)-[ochrm:LOCATED_ON]->(chrm)                

            CREATE (gchrm:GenomicLocation {primaryKey: row.genomicLocationUUID})
              SET gchrm.start = apoc.number.parseInt(row.start),
                gchrm.end = apoc.number.parseInt(row.end),
                gchrm.assembly = row.assembly,
                gchrm.strand = row.strand,
                gchrm.chromosome = row.chromosomeNumber

            CREATE (o)-[of:ASSOCIATION]->(gchrm)
            CREATE (gchrm)-[ofc:ASSOCIATION]->(chrm)
            CREATE (gchrm)-[ao:ASSOCIATION]->(a)"""

    transcript_alternate_id_query_template = """
            USING PERIODIC COMMIT %s
                LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            
                MATCH (g:Gene {primaryKey:row.curie})
                  SET g.gff3ID = row.gff3ID"""

    transcript_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (g:Gene {gff3ID: row.parentId})
                MATCH (so:SOTerm {name: row.featureType})

                MERGE (t:Transcript {primaryKey:row.curie})
                    ON CREATE SET t.gff3ID = row.gff3ID,
                        t.dataProvider = row.dataProvider,
                        t.name = row.name,
                        t.synonym = row.synonym           
                
               MERGE (t)<-[tso:TRANSCRIPT_TYPE]-(so)
               MERGE (g)<-[gt:TRANSCRIPT]-(t)"""


    chromosomes_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MERGE (chrm:Chromosome {primaryKey: row.chromosomeNumber}) """


    genomic_locations_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Transcript {primaryKey: row.curie})
            MATCH (chrm:Chromosome {primaryKey: row.chromosomeNumber})
            
            MERGE (a:Assembly {primaryKey: row.assembly})
             ON CREATE SET a.dataProvider = row.dataProvider
            
            CREATE (o)-[ochrm:LOCATED_ON]->(chrm)                

            CREATE (gchrm:GenomicLocation {primaryKey: row.genomicLocationUUID})
              SET gchrm.start = apoc.number.parseInt(row.start),
                gchrm.end = apoc.number.parseInt(row.end),
                gchrm.assembly = row.assembly,
                gchrm.strand = row.strand,
                gchrm.chromosome = row.chromosomeNumber

            CREATE (o)-[of:ASSOCIATION]->(gchrm)
            CREATE (gchrm)-[ofc:ASSOCIATION]->(chrm)
            CREATE (gchrm)-[ao:ASSOCIATION]->(a)"""


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
        self.logger.info("Loading Transcript Data: %s", sub_type.get_data_provider())
        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()
        filepath = sub_type.get_filepath()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.transcript_alternate_id_query_template, commit_size,
             "transcript_gff3ID_data_" + sub_type.get_data_provider() + ".csv"],
            [self.transcript_query_template, commit_size,
             "transcript_data_" + sub_type.get_data_provider() + ".csv"],
            [self.chromosomes_query_template, commit_size,
             "transcript_data_chromosome_" + sub_type.get_data_provider() + ".csv"],
            [self.genomic_locations_query_template, commit_size,
             "transcript_genomic_locations_" + sub_type.get_data_provider() + ".csv"],
            [self.exon_query_template, commit_size,
             "exon_data_" + sub_type.get_data_provider() + ".csv"],
            [self.exon_genomic_locations_template, commit_size,
             "exon_genomic_location_data_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(filepath, batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("Transcript-{}: ".format(sub_type.get_data_provider()))


    def get_generators(self, filepath, batch_size):
        """Get Generators"""

        with open(filepath) as file_handle:
            transcript_maps = []
            gene_maps = []
            exon_maps = []
            counter = 0
            data_provider = ''
            assembly = ''
            for line in file_handle:
                counter = counter + 1
                transcript_map = {}
                gene_map = {}
                exon_map = {}

                curie = ''
                parent = ''
                gff3_id = ''
                synonym = ''
                name = ''

                transcript_types = ['mRNA', 'ncRNA', 'piRNA', 'lincRNA', 'miRNA', 'pre_miRNA', 'snoRNA', 'lnc_RNA',
                                    'tRNA', 'snRNA', 'rRNA', 'antisense_RNA', 'C_gene_segment',
                                    'V_gene_segment', 'pseudogene_attribute', 'snoRNA_gene', 'pseudogenic_transcript']
                possible_types = ['gene', 'exon','mRNA', 'ncRNA', 'piRNA', 'lincRNA', 'miRNA',
                                  'pre_miRNA', 'snoRNA', 'lnc_RNA', 'tRNA', 'snRNA', 'rRNA',
                                  'antisense_RNA', 'C_gene_segment', 'V_gene_segment',
                                  'pseudogene_attribute', 'snoRNA_gene', 'pseudogenic_transcript']
                gene_id = ''

                if line.startswith('#!'):
                    header_columns = line.split()
                    if line.startswith('#!assembly'):
                        assembly = header_columns[1]

                    elif line.startswith('#!data-source '):
                        data_provider = header_columns[1]
                        if data_provider == 'FlyBase':
                            data_provider = 'FB'
                        if data_provider == 'WormBase':
                            data_provider = 'WB'
                        if data_provider == 'RAT':
                            data_provider = 'RGD'
                elif line.startswith('##FASTA'):
                    break
                elif line.startswith('#'):
                    continue
                else:
                    columns = re.split(r'\t', line)
                    feature_type_name = columns[2].strip()
                    if feature_type_name in possible_types:
                        column8 = columns[8]
                        notes = "_".join(column8.split())
                        kvpairs = re.split(';', notes)
                        if kvpairs is not None:
                            for pair in kvpairs:
                                if "=" in pair:
                                    key = pair.split("=")[0]
                                    value = pair.split("=")[1]
                                    if key == 'ID':
                                        if data_provider == 'WB':
                                            if ":" in value:
                                                gff3_id = value.split(":")[1]
                                            else:
                                                gff3_id = value
                                        else:
                                            gff3_id = value
                                    if key == 'gene_id':
                                        gene_id = value
                                    if key == 'Parent':
                                        if data_provider == 'WB':
                                            parent = value.split(":")[1]
                                        else:
                                            parent = value
                                    if key == 'Name':
                                        name = value
                                    if key == 'transcript_id':
                                        if value.startswith("FB:") or data_provider == 'MGI':
                                            synonym = gff3_id
                                            if ":" in value and data_provider == 'MGI':
                                                gff3_id = value.split(":")[1]
                                            else:
                                                gff3_id = value
                                    if key == 'curie':
                                        curie = value

                                if self.test_object.using_test_data() is True:
  
                                    is_it_test_entry = self.test_object.check_for_test_id_entry(curie)

                                    if is_it_test_entry is False:
                                        is_it_test_entry = self.test_object.check_for_test_id_entry(parent)
                                        if is_it_test_entry is False:
                                            is_it_test_entry = self.test_object.check_for_test_id_entry(gene_id)

                                            if is_it_test_entry is True:
                                                counter = counter - 1
                                            continue
                        if feature_type_name in transcript_types:
                            transcript_map.update({'curie' : curie})
                            transcript_map.update({'parentId': parent})
                            transcript_map.update({'gff3ID': gff3_id})
                            transcript_map.update({'genomicLocationUUID': str(uuid.uuid4())})
                            transcript_map.update({'chromosomeNumber': columns[0]})
                            transcript_map.update({'featureType': feature_type_name})
                            transcript_map.update({'start': columns[3]})
                            transcript_map.update({'dataProvider': data_provider})
                            transcript_map.update({'end': columns[4]})
                            transcript_map.update({'assembly': assembly})
                            transcript_map.update({'dataProvider': data_provider})
                            transcript_map.update({'name': name})
                            transcript_map.update({'synonym': synonym})
                            if assembly is None:
                                assembly = 'assembly_unlabeled_in_gff3_header'
                                transcript_map.update({'assembly': assembly})
                            transcript_maps.append(transcript_map)

                        elif feature_type_name == 'gene':
                            gene_map.update({'curie': curie})
                            gene_map.update({'parentId': parent})
                            gene_map.update({'gff3ID': gff3_id})
                            gene_map.update({'synonym': synonym})
                            gene_maps.append(gene_map)
                        elif feature_type_name == 'exon':
                            exon_map.update({'parentId': parent})
                            exon_map.update({'gff3ID': str(uuid.uuid4())})
                            exon_map.update({'genomicLocationUUID': str(uuid.uuid4())})
                            exon_map.update({'chromosomeNumber': columns[0]})
                            exon_map.update({'featureType': feature_type_name})
                            exon_map.update({'start': columns[3]})
                            exon_map.update({'dataProvider': data_provider})
                            exon_map.update({'end': columns[4]})
                            exon_map.update({'assembly': assembly})
                            exon_map.update({'dataProvider': data_provider})
                            exon_map.update({'name': name})
                            exon_map.update({'synonym': synonym})
                            if assembly is None or assembly == '':
                                assembly = 'assembly_unlabeled_in_gff3_header'
                                exon_map.update({'assembly': assembly})
                                transcript_map.update({'assembly': assembly})
                            exon_maps.append(exon_map)
                        else:
                            continue

                if counter == batch_size:
                    counter = 0

                    yield [gene_maps,
                          transcript_maps,
                          transcript_maps,
                          transcript_maps,
                          exon_maps,
                          exon_maps]
                    transcript_maps = []
                    gene_maps = []
                    exon_maps = []


            if counter > 0:
                yield [gene_maps,
                      transcript_maps,
                      transcript_maps,
                      transcript_maps,
                      exon_maps,
                      exon_maps]
