from files import S3File, TXTFile
from .obo_parser import parseOBO

class DOExt(object):

    def get_data(self, test_object):
        path = "tmp";
        S3File("mod-datadumps", "disease-ontology.obo", path).download()
        do_data = TXTFile(path + "/disease-ontology.obo").get_data()
        parsed_line = parseOBO(do_data)
        list_to_return = []
        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.
            isasWithoutNames = []
            do_syns = line.get('synonym')
            syns = []
            xrefs = []
            local_id = None
            global_id = None
            complete_url = None
            xref = None
            xref_urls = {}
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
            do_xrefs = line.get('xref')
            if do_xrefs is not None:
                if isinstance(do_xrefs, (list, tuple)):
                    for xrefId in do_xrefs:
                        if ":" in xrefId:
                            local_id = xrefId.split(":")[1].strip()
                            prefix = xrefId.split(":")[0].strip()
                            complete_url = self.get_complete_url(local_id, xrefId)
                            xrefs.append(xref)
                            xref_urls = {"doid": line['id'], "xrefId": xrefId, "local_id": local_id, "prefix": prefix, "complete_url": complete_url}
                else:
                    if ":" in do_xrefs:
                        local_id = do_xrefs.split(":")[1].strip()
                        prefix = do_xrefs.split(":")[0].strip()
                        xrefs.append(do_xrefs)
                        complete_url = self.get_complete_url(local_id, do_xrefs)
                        xref_urls = {"doid": line['id'], "xrefId": do_xrefs, "local_id": local_id, "prefix": prefix, "complete_url": complete_url}
            if xrefs is None:
                xrefs = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            do_is_as = line.get('is_a')
            if do_is_as is None:
                do_is_as = []
                isasWithoutNames = []
            else:
                if isinstance(do_is_as, (list, tuple)):
                    for isa in do_is_as:
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
                'xrefs': xrefs,
                'rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=rat&acc_id='+line['id'],
                'mgi_link': 'http://www.informatics.jax.org/disease/'+line['id'],
                'wormbase_link': 'no_link_yet',
                'flybase_link': 'http://flybase.org/cgi-bin/cvreport.html?id='+line['id'],
                'zfin_link': 'https://zfin.org/'+line['id'],
                'human_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=human&acc_id='+line['id'],
                'doUrl': "http://www.disease-ontology.org/?id=" + line['id'],
                'doPrefix': "DOID",
                'xref_urls': xref_urls

            }
            list_to_return.append(dict_to_append)

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
