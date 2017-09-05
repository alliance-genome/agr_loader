from files import S3File
from .obo_parser import parseGOOBO

class DOExt(object):
    @staticmethod
    def get_data(testObject):
        path = "tmp";
        S3File("mod-datadumps", "disease-ontology.obo", path).download()
        parsed_line = parseGOOBO(path + "/disease-ontology.obo")
        list_to_return = []
        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.
            isasWithoutNames = []
            do_syns = line.get('synonym')
            syns = []
            xrefs = []
            #print (do_synonyms)
            if syns is None:
                syns = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            if do_syns is not None:
                if isinstance(do_syns, (list, tuple)):
                    for syn in do_syns:
                        syn = syn.split("\"")[1].strip()
                        syns.append(syn)
                else:
                    syn = do_syns.split("\"")[1].strip()
                    syns.append(syn)
            xrefs = line.get('xref')
            #print (do_synonyms)
            if xrefs is None:
                xrefs = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            do_is_as = line.get('is_a')
            #print (do_is_as)
            if do_is_as is None:
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
            if definition is None:
                definition = ""
            subset = line.get('subset')
            if subset is None:
                subset = ""
            is_obsolete = line.get('is_obsolete')
            if is_obsolete is None:
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
                'is_obsolete': is_obsolete,
                'subset': subset,
                'xrefs': xrefs

            }
            list_to_return.append(dict_to_append)

        return list_to_return
