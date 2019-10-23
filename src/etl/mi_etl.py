import logging
import urllib, json

from etl import ETL
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)



class MIETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        //Create the MITerm node and set properties. primaryKey is required.
        MERGE (g:MITerm:Ontology {primaryKey:row.identifier})
            SET g.label = row.label
            SET g.url = row.url
            SET g.definition = row.definition
        MERGE (g)-[ggmg:IS_A_PART_OF_CLOSURE]->(g)
    """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        generators = self.get_generators()

        query_list = [[MIETL.query_template, 10000, "mi_term_data.csv"]]

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        
    @staticmethod
    def add_miterm_url(identifier):
        mi_term_url_dict = {
            'MI:0465' : 'http://dip.doe-mbi.ucla.edu/',
            'MI:0469' : 'http://www.ebi.ac.uk/intact',
            'MI:0471' : 'http://mint.bio.uniroma2.it',
            'MI:0478' : 'http://flybase.org',
            'MI:0486' : 'http://www.uniprot.org',
            'MI:0487' : 'http://www.wormbase.org/',
            'MI:0670' : 'https://www.imexconsortium.org/',
            'MI:0903' : 'https://www.ebi.ac.uk/intact/',
            'MI:0917' : 'http://matrixdb.univ-lyon1.fr/',
            'MI:0974' : 'http://www.innatedb.ca/',
            'MI:1222' : 'http://www.mechanobio.info/',
            'MI:1262' : 'http://ophid.utoronto.ca/',
            'MI:1263' : 'http://www.molecularconnections.com',
            'MI:1264' : 'http://www.ntnu.no/home',
            'MI:1335' : 'http://www.agbase.msstate.edu/hpi/main.html',
            'MI:0463' : 'https://thebiogrid.org/'
        }

        return mi_term_url_dict.get(identifier)

    @staticmethod
    def adjust_database_names(name):
        mi_database_name_dict = {
            'flybase': 'FlyBase',
            'wormbase': 'WormBase',
            'biogrid': 'BioGRID',
            'imex': 'IMEx',
            'intact': 'IntAct',
            'mint': 'MINT',
            'dip': 'DIP',
            'iid': 'IID',
            'uniprot knowledge base': 'UniProtKB',
            'ntnu': 'NTNU',
            'molecular connections': 'Molecular Connections',
            'hpidb': 'HPIDB',
            'innatedb': 'InnateDB',
            'matrixdb': 'MatrixDB',
            'mbinfo': 'MBInfo',
            'bhf-ucl': 'BHF-UCL',
            'mpidb': 'MPIDB'
        }

        # Return the original if there is no entry in the dict.
        return mi_database_name_dict.get(name, name)

    @staticmethod
    def add_definition(term):
        try:
            return term['annotation']['definition'][0]
        except KeyError:
            return None

    def get_generators(self):

        #mi_term_ontology = None
        mi_term_ontology_full = None

        # TODO Make size configurable?
        logger.info('Downloading MI ontology terms via: https://www.ebi.ac.uk/ol/api/ontologies/mi/terms?size=500')

        response = urllib.request.urlopen("https://www.ebi.ac.uk/ols/api/ontologies/mi/terms?size=500")

        mi_term_ontology = json.loads(response.read().decode())

        logger.info('Determining total number of terms and pages to request...')
        total_terms = mi_term_ontology['page']['totalElements']
        total_pages = mi_term_ontology['page']['totalPages']

        logger.info('Requesting %s terms over %s pages.' % (total_terms, total_pages))

        processed_mi_list = []
        for i in range(total_pages):
            request_url = 'https://www.ebi.ac.uk/ols/api/ontologies/mi/terms?page=%s&size=500' % (i)
            logger.info('Retrieving terms from page %s of %s.' % (i+1, total_pages))

            response = urllib.request.urlopen(request_url)

            mi_term_ontology_full = json.loads(response.read().decode())

            for term in mi_term_ontology_full['_embedded']['terms']:
                if term['obo_id'] is not None: # Avoid weird "None" entry from MI ontology.

                    adjusted_label = self.adjust_database_names(term['label'])
                    if adjusted_label != term['label']:
                        logger.info('Updated MI database name: {} -> {}'.format(term['label'], adjusted_label))

                    dict_to_append = {
                            'identifier': term['obo_id'],
                            'label': adjusted_label,
                            'definition': self.add_definition(term),
                            'url': self.add_miterm_url(term['obo_id'])
                            }
                    processed_mi_list.append(dict_to_append)

        yield [processed_mi_list]
