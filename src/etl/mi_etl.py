import logging
logger = logging.getLogger(__name__)

import urllib, json

from etl import ETL
from transactors import CSVTransactor

class MIETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        //Create the MITerm node and set properties. primaryKey is required.
        MERGE (g:MITerm:Ontology {primaryKey:row.identifier})
            SET g.label = row.label
            SET g.url = row.url
            SET g.definition = row.definition
    """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        generators = self.get_generators()

        mi_file_query_list = [[MIETL.query_template, 10000, "mi_term_data.csv"]]
            
        CSVTransactor.execute_transaction(generators, mi_file_query_list)

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
            'MI:1262' : 'http://iid.ophid.utoronto.ca/iid/',
            'MI:1263' : 'http://www.molecularconnections.com',
            'MI:1264' : 'http://www.ntnu.no/home',
            'MI:1332' : 'http://www.ucl.ac.uk/functional-gene-annotation/psicquic/Tabs/bhf-ucl-dataset',
            'MI:1335' : 'http://www.agbase.msstate.edu/hpi/main.html',
            'MI:0463' : 'https://thebiogrid.org/'
        }

        return mi_term_url_dict.get(identifier)

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
        logger.info('Downloading MI ontology terms via: https://www.ebi.ac.uk/ols/api/ontologies/mi/terms?size=500')
        with urllib.request.urlopen("https://www.ebi.ac.uk/ols/api/ontologies/mi/terms?size=500") as url:
            mi_term_ontology = json.loads(url.read().decode())

        logger.info('Determining total number of terms and pages to request...')
        total_terms = mi_term_ontology['page']['totalElements']
        total_pages = mi_term_ontology['page']['totalPages']

        logger.info('Requesting %s terms over %s pages.' % (total_terms, total_pages))

        processed_mi_list = []
        for i in range(total_pages):
            request_url = 'https://www.ebi.ac.uk/ols/api/ontologies/mi/terms?page=%s&size=500' % (i)
            logger.info('Retrieving terms from page %s of %s.' % (i+1, total_pages))
            with urllib.request.urlopen(request_url) as url:
                mi_term_ontology_full = json.loads(url.read().decode())

                for term in mi_term_ontology_full['_embedded']['terms']:
                    if term['obo_id'] is not None: # Avoid weird "None" entry from MI ontology.
                        dict_to_append = {
                            'identifier' : term['obo_id'],
                            'label' : term['label'],
                            'definition' : self.add_definition(term),
                            'url' : self.add_miterm_url(term['obo_id'])
                        }
                        processed_mi_list.append(dict_to_append)

        yield [processed_mi_list]