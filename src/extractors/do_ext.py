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
            syns = line.get('synonym')
            xrefs = []
            #print (do_synonyms)
            if syns == None:
                syns = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            if isinstance(syns, (list, tuple)):
                for syn in syns:
                    syn = syn.split("\"")[1].strip()
                    print (syn + "list")
                    syns.append(syn)
            else:
                syn = syns.split("\"")[1].strip()
                syns.append(syn)
                print (syn)
            xrefs = line.get('xref')
            #print (do_synonyms)
            if xrefs == None:
                xrefs = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            do_is_as = line.get('is_a')
            #print (do_is_as)
            if do_is_as == None:
                do_is_as = []
                isasWithoutNames = []
            else:
                if isinstance(do_is_as, (list, tuple)):
                    for isa in do_is_as:
                        #print (isa)
                        isaWithoutName = isa.split("!")[0].strip()
                        isasWithoutNames.append(isaWithoutName)
                else:
                    isaWithoutName = do_is_as.split("!")[0].strip()
                    isasWithoutNames.append(isaWithoutName)

            definition = line.get('def')
            if definition == None:
                definition = ""
            subset = line.get('subset')
            if subset == None:
                subset = ""
            is_obsolete = line.get('is_obsolete')
            if is_obsolete == None:
                is_obsolete = ""

            dict_to_append = {
                'do_genes': [],
                'do_species': [],
                'name': line['name'],
                'do_synonyms': syns,
                'name_key': line['name'],
                'id': line['id'],
                'definition': definition,
                'category': 'do',
                'isas': isasWithoutNames,
                'is_obsolete':is_obsolete,
                'subset':subset

            }
            list_to_return.append(dict_to_append)

        return list_to_return
