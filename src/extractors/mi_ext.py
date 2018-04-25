import urllib.request, json

class MIExt(object):

    def get_data(self):

        mi_term_ontology = None

        print('Downloading MI ontology terms via: https://www.ebi.ac.uk/ols/api/ontologies/mi/terms')
        with urllib.request.urlopen("https://www.ebi.ac.uk/ols/api/ontologies/mi/terms") as url:
            mi_term_ontology = json.loads(url.read().decode())

        # Transforming mi_term_ontology into a more parseable dictionary.
        processed_mi_dict = {}
        for terms in mi_term_ontology['_embedded']['terms']:
            if terms['obo_id'] is not None: # Avoid weird "None" entry from MI ontology.
                processed_mi_dict[terms['obo_id']] = terms['label']

        return processed_mi_dict