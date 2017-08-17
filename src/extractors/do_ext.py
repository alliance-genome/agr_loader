from files import *
import sys
import re
from .obo_parser import *

class DOExt:
    @staticmethod
    def get_data(testObject):
        path = "tmp";
        S3File("mod-datadumps", "disease-ontology.obo", path).download()
        parsed_line = parseGOOBO(path + "/disease-ontology.obo")
        list_to_return = []
        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.
            isasWithoutNames = []
            do_synonyms = line.get('synonym')
            #print (do_synonyms)
            if do_synonyms == None:
                do_synonyms = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            do_is_as = line.get('is_a')
            #print (do_is_as)
            if do_is_as == None:
                do_is_as = []
                isasWithoutNames = []
            else:
                for isa in do_is_as:
                    #print (isa)
                    isaWithoutName = isa.split("!")[0]
                    isasWithoutNames.append(isaWithoutName)

            dict_to_append = {
                'do_genes': [],
                'do_species': [],
                'name': line['name'],
                'do_synonyms': do_synonyms,
                'name_key': line['name'],
                'id': line['id'],
                'category': 'do',
                'isas': do_is_as
            }
            list_to_return.append(dict_to_append)

        return list_to_return

