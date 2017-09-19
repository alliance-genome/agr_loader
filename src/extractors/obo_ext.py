from files import S3File, TXTFile
from .obo_parser import parseOBO

class OExt(object):

    def get_data(self, test_object, filename, prefix):
        path = "tmp";
        S3File("mod-datadumps", prefix+filename, path).download()
        o_data = TXTFile(path + filename).get_data()
        parsed_line = parseOBO(o_data)
        list_to_return = []
        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.
            isasWithoutNames = []
            o_syns = line.get('synonym')
            syns = []
            xrefs = []
            local_id = None
            global_id = None
            complete_url = None
            xref = None
            xref_urls = []
            defLinksProcessed =[]
            defLinks =[]
            is_obsolete = "false"
            id = line['id']
            print (id)
            prefix = id.split(":")[0]
            print (prefix)
            if syns is None:
                syns = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            if o_syns is not None:
                if isinstance(o_syns, (list, tuple)):
                    for syn in o_syns:
                        syn = syn.split("\"")[1].strip()
                        syns.append(syn)
                else:
                    syn = o_syns.split("\"")[1].strip()
                    syns.append(syn)
            o_xrefs = line.get('xref')
            if o_xrefs is not None:
                if isinstance(o_xrefs, (list, tuple)):
                    for xrefId in o_xrefs:
                        if ":" in xrefId:
                            local_id = xrefId.split(":")[1].strip()
                            prefix = xrefId.split(":")[0].strip()
                            complete_url = self.get_complete_url(local_id, xrefId)
                            xrefs.append(xref)
                            xref_urls.append({"oid": line['id'], "xrefId": xrefId, "local_id": local_id, "prefix": prefix, "complete_url": complete_url})
                else:
                    if ":" in o_xrefs:
                        local_id = o_xrefs.split(":")[1].strip()
                        prefix = o_xrefs.split(":")[0].strip()
                        xrefs.append(o_xrefs)
                        complete_url = self.get_complete_url(local_id, o_xrefs)
                        xref_urls.append({"oid": line['id'], "xrefId": o_xrefs, "local_id": local_id, "prefix": prefix, "complete_url": complete_url})
            if xrefs is None:
                xrefs = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing
            o_is_as = line.get('is_a')
            if o_is_as is None:
                o_is_as = []
                isasWithoutNames = []
            else:
                if isinstance(o_is_as, (list, tuple)):
                    for isa in o_is_as:
                        isaWithoutName = isa.split("!")[0].strip()
                        isasWithoutNames.append(isaWithoutName)
                else:
                    isaWithoutName = o_is_as.split("!")[0].strip()
                    isasWithoutNames.append(isaWithoutName)

            definition = line.get('def')
            if definition is not None:
                defText = definition.split("\"")[1]
                if "[" in definition:
                    defLinks = definition.split("\"")[2]
                    defLinks = defLinks.rstrip("]")[1:]
                    defLinks = defLinks.replace("url:", "")
                    defLinks = defLinks.replace("\\:", ":")
                    if "," in defLinks:
                        defLinks = defLinks.split(",")
                        for link in defLinks:
                            link = link[1:]
                            link = link.replace("url:", "")
                            link = link.replace("\\:", ":")
                            defLinksProcessed.append(link)
                    else:
                        defLinks = defLinks.replace("[", "")
                        defLinks = defLinks.replace("url:", "")
                        defLinks = defLinks.replace("\\:", ":")
                        defLinksProcessed.append(defLinks)
            else:
                definition = ""
            subset = line.get('subset')
            if subset is not None:
                if "," in subset:
                    subset = subset.split(",")
            else:
                subset = ""
            is_obsolete = line.get('is_obsolete')
            if is_obsolete is None:
                is_obsolete = "false"

            dict_to_append = {
                'o_genes': [],
                'o_species': [],
                'name': line['name'],
                'o_synonyms': syns,
                'name_key': line['name'],
                'id': line['id'],
                'definition': definition,
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
                'oUrl': "http://www.disease-ontology.org/?id=" + line['id'],
                'oPrefix': prefix,
                'xref_urls': xref_urls,
                'defText': defText,
                'defLinksProcessed': defLinksProcessed

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
