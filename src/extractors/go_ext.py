from files import S3File, TXTFile
from .obo_parser import parseOBO

class GOExt(object):

    @staticmethod
    def get_data(testObject):
        path = "tmp";
        S3File("mod-datadumps/data", "go.obo", path).download()
        go_data = TXTFile(path + "/go.obo").get_data()
        parsed_line = parseOBO(go_data)
        list_to_return = []
        for line in parsed_line: # Convert parsed obo term into a schema-friendly AGR dictionary.
            isasWithoutNames = []
            syns = []
            go_synonyms = line.get('synonym')
            xrefs = []
            if go_synonyms is None:
                go_synonyms = []
                syns = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            if go_synonyms is not None:
                if isinstance(go_synonyms, (list, tuple)):
                    for syn in go_synonyms:
                        syn = syn.split("\"")[1].strip()
                        syns.append(syn)
                else:
                    syn = go_synonyms.split("\"")[1].strip()
                    syns.append(syn)
            xrefs = line.get('xref')
            #print (do_synonyms)
            if xrefs is None:
                xrefs = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            go_is_as = line.get('is_a')
            #print (do_is_as)
            if go_is_as is None:
                go_is_as = []
                isasWithoutNames = []
            else:
                if isinstance(go_is_as, (list, tuple)):
                    for isa in go_is_as:
                        #print (isa)
                        isaWithoutName = isa.split("!")[0].strip()
                        isasWithoutNames.append(isaWithoutName)
                else:
                    isaWithoutName = go_is_as.split("!")[0].strip()
                    isasWithoutNames.append(isaWithoutName)
            subset = line.get('subset')
            if subset is None:
                subset = ""
            is_obsolete = line.get('is_obsolete')
            if is_obsolete is None:
                is_obsolete = ""
            definition = line.get('def')
            if definition is None:
                definition = ""
            dict_to_append = {
                'go_genes': [],
                'go_species': [],
                'name': line['name'],
                'description': definition,
                'go_type': line['namespace'],
                'go_synonyms': syns,
                'name_key': line['name'],
                'id': line['id'],
                'href': 'http://amigo.geneontology.org/amigo/term/' + line['id'],
                'category': 'go',
                'is_a': isasWithoutNames,
                'is_obsolete': is_obsolete,
                'xrefs': xrefs
            }
            list_to_return.append(dict_to_append)

        if testObject.using_test_data() is True:
            filtered_dict = []
            for entry in list_to_return:
                if testObject.check_for_test_go_entry(entry['id']) is True:
                    filtered_dict.append(entry)
                else:
                    continue
            return filtered_dict
        else:
            return list_to_return