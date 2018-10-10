import urllib.request, json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class MIExt(object):

    # TODO Replace this approach with links to crossrefs.
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
            'MI:1332' : 'https://www.ebi.ac.uk/GOA/CVI',
            'MI:1335' : 'http://www.agbase.msstate.edu/hpi/main.html',
            'MI:0463' : 'https://thebiogrid.org/'
        }

        return mi_term_url_dict.get(identifier)

    def get_data(self):

        mi_term_ontology = None
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

                for terms in mi_term_ontology_full['_embedded']['terms']:
                    if terms['obo_id'] is not None: # Avoid weird "None" entry from MI ontology.
                        dict_to_append = {
                            'identifier' : terms['obo_id'],
                            'label' : terms['label'],
                            'definition' : terms.get(['annotation']['definition'][0]),
                            'url' : self.add_miterm_url(terms['obo_id'])
                        }
                        processed_mi_list.append(dict_to_append)

        return processed_mi_list
