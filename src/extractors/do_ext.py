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
            go_synonyms = line.get('synonym')
            if go_synonyms == None:
                go_synonyms = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            #print (line)
            dict_to_append = {
                'do_genes': [],
                'do_species': [],
                'name': line['name'],
                'do_synonyms': go_synonyms,
                'name_key': line['name'],
                'id': line['id'],
                'category': 'do'
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


        # creating_term = None
        #
        # for line in do_data:
        #     line = line.strip()
        #
        #     if line == "[Term]":
        #         creating_term = True
        #     elif line == '': # Skip blank lines
        #         continue
        #     elif creating_term:
        #         key = (line.split(":")[0]).strip()
        #         value = ("".join(":".join(line.split(":")[1:]))).strip()
        #
        #         if key == "id":
        #             creating_term = value
        #             do_dataset[creating_term] = {"id": value}
        #             do_dataset[creating_term]['do_genes'] = [] # Empty dictionaries to receive entries later.
        #             do_dataset[creating_term]['do_species'] = []
        #         elif key == "name":
        #             do_dataset[creating_term]['name'] = value
        #         else:
        #             if key == "synonym":
        #                 if value.split(" ")[-2] == "EXACT":
        #                     value = (" ".join(value.split(" ")[:-2]))[1:-1]
        #                 else:
        #                     continue
        #             if key == "def":
        #                 m = re.search('\"(.+)\"', value)
        #                 value = m.group(1)
        #
        #             if key in do_dataset[creating_term]:
        #                 do_dataset[creating_term][key].append(value)
        #             else:
        #                 do_dataset[creating_term][key] = [value]

        return do_dataset
