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
            syns = []
            do_synonyms = line.get('synonym')
            #print (do_synonyms)
            if do_synonyms == None:
                do_synonyms = []
                syns = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            if isinstance(do_synonyms, (list, tuple)):
                for syn in do_synonyms:
                    syn = syn.split("[")[0]
                    syns.append(syn)
            else:
                syn = do_synonyms.split("[")[0]
                syns.append(syn)

            do_is_as = line.get('is_a')
            do_crossreferences = line.get('dbxref')
            #print (do_synonyms)
            if do_crossreferences == None:
                do_crossreferences = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            do_is_as = line.get('is_a')
            #print (do_is_as)
            if do_is_as == None:
                do_is_as = []
                isasWithoutNames = []
            else:
                if isinstance(do_is_as, (list, tuple)):
                    for isa in do_is_as:
                        #print (isa)
                        isaWithoutName = isa.split("!")[0]
                        isasWithoutNames.append(isaWithoutName)
                else:
                    isaWithoutName = do_is_as.split("!")[0]
                    isasWithoutNames.append(isaWithoutName)

            definition = ''
            if definition is not None:
                definition = line.get('def')
            dict_to_append = {
                'do_genes': [],
                'do_species': [],
                'name': line['name'],
                'do_synonyms': syns,
                'name_key': line['name'],
                'id': line['id'],
                'definition': definition,
                'category': 'do',
                'isas': isasWithoutNames
            }
            list_to_return.append(dict_to_append)

        return list_to_return

    def func(arg):
        if not isinstance(arg, (list, tuple)):
            arg = [arg]
        return arg