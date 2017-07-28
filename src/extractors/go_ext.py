from files import *
from .obo_parser import *

import re

class GOExt:

    @staticmethod
    def get_data():
        path = "tmp";
        S3File("mod-datadumps/data", "go.obo", path).download()
        parsed_line = parseGOOBO(path + "/go.obo")
        list_to_return = []
        for line in parsed_line: # Convert parsed obo term into a schema-friendly AGR dictionary.
            dict_to_append = {
                'go_genes': [],
                'go_species': [],
                'name': line['name'],
                'description': line['def'],
                'go_type': line['namespace'],
                'go_synonyms': line.get('synonym'),
                'name_key': line['name'],
                'id': line['id'],
                'href': 'http://amigo.geneontology.org/amigo/term/' + line['id'],
                'category': 'go'
            }
            list_to_return.append(dict_to_append)

        return list_to_return