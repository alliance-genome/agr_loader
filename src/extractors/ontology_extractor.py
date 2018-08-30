import urllib.request, json
import logging

logger = logging.getLogger(__name__)

class OntologyExt(object):

    def get_data(self, ontName):

        term_ontology = None
        term_ontology_full = None

        # TODO Make size configurable?
        logger.info('Downloading ontology terms via: https://www.ebi.ac.uk/ols/api/ontologies/' + ontName + '/terms?size=500')
        with urllib.request.urlopen("https://www.ebi.ac.uk/ols/api/ontologies/' + ontName + '/terms?size=500'") as url:
            term_ontology = json.loads(url.read().decode())

        logger.info('Determining total number of terms and pages to request...')
        total_terms = term_ontology['page']['totalElements']
        total_pages = term_ontology['page']['totalPages']

        logger.info('Requesting %s terms over %s pages.' % (total_terms, total_pages))

        processed_list = []
        for i in range(total_pages):
            request_url = 'https://www.ebi.ac.uk/ols/api/ontologies/' + ontName + '/terms?size=500' % (i)
            logger.info('Retrieving terms from page %s of %s.' % (i+1, total_pages))
            with urllib.request.urlopen(request_url) as url:
                term_ontology_full = json.loads(url.read().decode())

                for terms in term_ontology_full['_embedded']['terms']:
                    if terms['obo_id'] is not None: # Avoid weird "None" entry from ontName ontology.
                        dict_to_append = {
                            'identifier' : terms['obo_id'],
                            'label' : terms['label']
                        }
                        processed_list.append(dict_to_append)

        return processed_list
