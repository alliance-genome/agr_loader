from files import *
from .obo_parser import *

import re

class GOExt:

    @staticmethod
    def get_data(testObject):
        path = "tmp";
        S3File("mod-datadumps/data", "go.obo", path).download()
        parsed_line = parseGOOBO(path + "/go.obo")
        list_to_return = []
        for line in parsed_line: # Convert parsed obo term into a schema-friendly AGR dictionary.
            go_synonyms = line.get('synonym')
            if go_synonyms == None:
                go_synonyms = [] # Set the synonyms to an empty array if None. Necessary for Neo4j parsing.
            dict_to_append = {
                'go_genes': [],
                'go_species': [],
                'name': line['name'],
                'description': line['def'],
                'go_type': line['namespace'],
                'go_synonyms': go_synonyms,
                'name_key': line['name'],
                'id': line['id'],
                'href': 'http://amigo.geneontology.org/amigo/term/' + line['id'],
                'category': 'go'
            }
            list_to_return.append(dict_to_append)

        if testObject.using_test_data() == True:
            filtered_dict = []
            for entry in list_to_return:
                if testObject.check_for_test_go_entry(entry['id']) == True:
                    filtered_dict.append(entry)
                else:
                    continue
            return filtered_dict
        else:
            return list_to_return