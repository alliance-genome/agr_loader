from files import S3File, TXTFile
from .obo_parser import parseOBO

class GOExt(object):

    def get_data(self, testObject):
        path = "tmp";
        S3File("mod-datadumps", "go.obo", path).download()
        go_data = TXTFile(path + "go.obo").get_data()
        parsed_line = parseOBO(go_data)
        list_to_return = []
        for line in parsed_line: # Convert parsed obo term into a schema-friendly AGR dictionary.
            isasWithoutNames = []
            syns = []
            go_synonyms = line.get('synonym')
            xrefs = []
            xref = None
            xref_urls = []
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
            # TODO: lift these xref, synonym sections out into a callable method for both go and do and other ontologies.
            go_xrefs = line.get('xref')
            if go_xrefs is not None:
                if isinstance(go_xrefs, (list, tuple)):
                    for xrefId in go_xrefs:
                        if ":" in xrefId:
                            local_id = xrefId.split(":")[1].strip()
                            prefix = xrefId.split(":")[0].strip()
                            complete_url = self.get_complete_url(local_id, xrefId)
                            xrefs.append(xref)
                            xref_urls.append({"goid": line['id'], "xrefId": xrefId, "local_id": local_id, "prefix": prefix, "complete_url": complete_url})
                else:
                    if ":" in go_xrefs:
                        local_id = go_xrefs.split(":")[1].strip()
                        prefix = go_xrefs.split(":")[0].strip()
                        xrefs.append(go_xrefs)
                        complete_url = self.get_complete_url(local_id, go_xrefs)
                        xref_urls.append({"goid": line['id'], "xrefId": go_xrefs, "local_id": local_id, "prefix": prefix, "complete_url": complete_url})
            if xrefs is None:
                xrefs = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
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
                is_obsolete = "false"
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
                'xrefs': xrefs,
                'xref_urls': xref_urls
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

    def get_complete_url (self, local_id, global_id):

        complete_url = None

        if 'OMIM' in global_id:
            complete_url = 'https://www.omim.org/entry/' + local_id
        if 'ORDO' in global_id:
            complete_url = 'http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=EN&Expert=' +local_id
        if 'MESH' in global_id:
            complete_url = 'https://www.ncbi.nlm.nih.gov/mesh/' + local_id
        if 'EFO' in global_id:
            complete_url = 'http://www.ebi.ac.uk/efo/EFO_' + local_id
        if 'KEGG' in global_id:
            complete_url ='http://www.genome.jp/dbget-bin/www_bget?map' +local_id
        if 'NCI' in global_id:
            complete_url = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=' + global_id

        return complete_url
