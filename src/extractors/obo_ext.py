import uuid as id
from files import S3File, TXTFile
from .obo_parser import parseOBO

class OExt(object):

    def get_data(self, testObject, filename, prefix):
        path = "tmp";
        S3File("mod-datadumps"+prefix, filename, path).download()
        o_data = TXTFile(path + "/"+filename).get_data()
        parsed_line = parseOBO(o_data)
        list_to_return = []
        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.
            isasWithoutNames = []
            o_syns = line.get('synonym')
            syns = []
            xrefs = []
            complete_url = None
            xref = None
            xref_urls = []
            local_id = None
            defLinksProcessed = []
            defText = None
            defLinks = []
            subset = []
            newSubset = None
            definition = ""
            is_obsolete = "false"
            ident = line['id']
            prefix = ident.split(":")[0]
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
                            #TODO UUID slows down DO loader to 10 r/s!!
                            uuid = str(id.uuid4())
                            xrefs.append(xref)
                            xref_urls.append({"uuid": uuid, "primaryKey": prefix + local_id + "ontology_provided_cross_reference", "oid": line['id'], "xrefId": xrefId, "local_id": local_id, "prefix": prefix, "complete_url": complete_url, "crossRefType": "ontology_provided_cross_reference"})
                else:
                    if ":" in o_xrefs:
                        local_id = o_xrefs.split(":")[1].strip()
                        prefix = o_xrefs.split(":")[0].strip()
                        uuid = str(id.uuid4())
                        xrefs.append(o_xrefs)
                        complete_url = self.get_complete_url(local_id, o_xrefs)
                        xref_urls.append({"uuid": uuid, "primaryKey": prefix + local_id + "ontology_provided_cross_reference", "oid": line['id'], "xrefId": o_xrefs, "local_id": local_id, "prefix": prefix, "complete_url": complete_url, "crossRefType": "ontology_provided_cross_reference"})
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
            defLinks = ""
            defLinksProcessed = []
            if definition is None:
                definition = ""
            else:
                if definition is not None and "\"" in definition:
                    defText = definition.split("\"")[1].strip()
                    if "[" in definition.split("\"")[2].strip():
                        defLinks = definition.split("\"")[2].strip()
                        defLinks = defLinks.rstrip("]").replace("[", "")
                        defLinks = defLinks.replace("url:www", "http://wwww")
                        defLinks = defLinks.replace("url:", "")
                        defLinks = defLinks.replace("URL:", "")
                        defLinks = defLinks.replace("\\:", ":")

                        if "," in defLinks:
                            defLinks = defLinks.split(",")
                            for link in defLinks:
                                if link.strip().startswith("http"):
                                    defLinksProcessed.append(link)
                        else:
                            if defLinks.strip().startswith("http"):
                                defLinksProcessed.append(defLinks)
                else:
                    definition = defText
            if definition is None:
                definition = ""

            newSubset = line.get('subset')
            if isinstance(newSubset, (list, tuple)):
                subset = newSubset
            else:
                if newSubset is not None:
                    subset.append(newSubset)
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
                #TODO: fix links to not be passed for each ontology load.
                'rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=All&x=1&acc_id='+line['id']+'#annot',
                'rgd_all_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=All&x=1&acc_id=' + line['id'] + '#annot',
                'rat_only_rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=Rat&x=1&acc_id=' +line['id'] + '#annot',
                'human_only_rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=Human&x=1&acc_id=' +line['id'] + '#annot',
                'mgi_link': 'http://www.informatics.jax.org/disease/'+line['id'],
                'wormbase_link': 'http://www.wormbase.org/resources/disease/'+line['id'],
                'flybase_link': 'http://flybase.org/cgi-bin/cvreport.html?id='+line['id'],
                'zfin_link': 'https://zfin.org/'+line['id'],
                'oUrl': "http://www.disease-ontology.org/?id=" + line['id'],
                'oPrefix': prefix,
                'xref_urls': xref_urls,
                'defText': defText,
                'defLinksProcessed': defLinksProcessed,
                'oboFile': prefix,
                'href': 'http://amigo.geneontology.org/amigo/term/' + line['id'],
                'category': 'go',
                'o_type': line.get('namespace'),

            }
            list_to_return.append(dict_to_append)

        # if testObject.using_test_data() is True:
        #     filtered_dict = []
        #     for entry in list_to_return:
        #         if testObject.check_for_test_ontology_entry(entry['id']) is True:
        #             filtered_dict.append(entry)
        #         else:
        #             continue
        #     return filtered_dict
        # else:
        return list_to_return

    #TODO: add these to resourceDescriptors.yaml and remove hardcoding.
    def get_complete_url (self, local_id, global_id):

        complete_url = None

        if 'OMIM' in global_id:
            complete_url = 'https://www.omim.org/entry/' + local_id
        if 'OMIM:PS' in global_id:
            complete_url = 'https://www.omim.org/phenotypicSeries/' + local_id
        if 'ORDO' in global_id:
            complete_url = 'http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=EN&Expert=' +local_id
        if 'MESH' in global_id:
            complete_url = 'https://www.ncbi.nlm.nih.gov/mesh/' + local_id
        if 'EFO' in global_id:
            complete_url = 'http://www.ebi.ac.uk/efo/EFO_' + local_id
        if 'KEGG' in global_id:
            complete_url ='http://www.genome.jp/dbget-bin/www_bget?map' +local_id
        if 'NCI' in global_id:
            complete_url = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=' + local_id

        return complete_url