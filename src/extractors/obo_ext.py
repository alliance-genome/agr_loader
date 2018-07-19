import uuid as id
from files import S3File, TXTFile
from services import CreateCrossReference
from .obo_parser import parseOBO
from ontobio import OntologyFactory

class OExt(object):


    def get_data(self, testObject, path):

        savepath = "tmp";
        fullpath = savepath + "/" + path
        saved_path = S3File(path, savepath).download()
        # o_data = TXTFile(fullpath).get_data()
        # parsed_line = parseOBO(o_data)
        ont = OntologyFactory().create(saved_path)

        parsed_line = ont.graph.copy().node
        for k, line in parsed_line.items():  # Convert parsed obo term into a schema-friendly AGR dictionary.
            node = ont.graph.node[k]
            if len(node) == 0:
                continue

            ### Switching id to curie form and saving URI in "uri" - might wildly break things later on???
            node["uri"] = node["id"]
            node["id"] = k

            isasWithoutNames = []
            relationships = node.get('relationship')
            partofsWithoutNames = []
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
            ident = k
            prefix = ident.split(":")[0]
            if syns is None:
                syns = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing

            if "meta" in node:
                if "synonyms" in node["meta"]:
                    # if isinstance(o_syns, (list, tuple)):
                    #     for syn in o_syns:
                    #         syn = syn.split("\"")[1].strip()
                    #         syns.append(syn)
                    # else:
                    #     syn = o_syns.split("\"")[1].strip()
                    #     syns.append(syn)
                    syns = [s["val"] for s in node["meta"]["synonyms"]]
                if "xrefs" in node["meta"]:

                    o_xrefs = node["meta"].get('xrefs')
                    if o_xrefs is not None:
                        # if isinstance(o_xrefs, (list, tuple)):
                        # if isinstance(o_xrefs, list):
                        for xrefIdDict in o_xrefs:
                            xrefId = xrefIdDict["val"]
                            if ":" in xrefId:
                                local_id = xrefId.split(":")[1].strip()
                                prefix = xrefId.split(":")[0].strip()
                                complete_url = self.get_complete_url_ont(local_id, xrefId)
                                generated_xref = CreateCrossReference.get_xref(local_id, prefix, "ontology_provided_cross_reference",
                                                              "ontology_provided_cross_reference", xrefId, complete_url,
                                                              xrefId + "ontology_provided_cross_reference")
                                generated_xref["oid"] = ident
                                xref_urls.append(generated_xref)
                        else:
                            if ":" in o_xrefs:
                                local_id = o_xrefs.split(":")[1].strip()
                                prefix = o_xrefs.split(":")[0].strip()
                                uuid = str(id.uuid4())
                                complete_url = self.get_complete_url_ont(local_id, o_xrefs)
                                generated_xref = CreateCrossReference.get_xref(local_id, prefix, "ontology_provided_cross_reference", "ontology_provided_cross_reference", o_xrefs, complete_url, o_xrefs)
                                generated_xref["oid"] = ident
                                xref_urls.append(generated_xref)
                if node["meta"].get('is_obsolete'):
                    is_obsolete = "true"
                elif node["meta"].get('deprecated'):
                    is_obsolete = "true"
            if xrefs is None:
                xrefs = []  # Set the synonyms to an empty array if None. Necessary for Neo4j parsing

            all_parents = ont.parents(k)
            all_parents.append(k)
            all_parents_subont = ont.subontology(all_parents) # Improves performance when traversing relations

            # o_is_as = node.get('is_a')
            # if o_is_as is None:
            #     o_is_as = []
            #     isasWithoutNames = []
            # else:
            #     if isinstance(o_is_as, (list, tuple)):
            #         for isa in o_is_as:
            #             isaWithoutName = isa.split("!")[0].strip()
            #             isasWithoutNames.append(isaWithoutName)
            #     else:
            #         isaWithoutName = o_is_as.split("!")[0].strip()
            #         isasWithoutNames.append(isaWithoutName)
            # if relationships:
            #     if isinstance(relationships, (list, tuple)):
            #         for relationship in relationships:
            #             relWithoutName = relationship.split("!")[0].strip()
            #             relType, relID = relWithoutName.split(" ")
            #             if relType == "part_of":
            #                 partofsWithoutNames.append(relID)
            #     else:
            #         relWithoutName = relationships.split("!")[0].strip()
            #         relType, relID = relWithoutName.split(" ")
            #         if relType == "part_of":
            #             partofsWithoutNames.append(relID)
            isasWithoutNames = all_parents_subont.parents(k, relations=['subClassOf'])
            partofsWithoutNames = all_parents_subont.parents(k, relations=['BFO:0000050'])
            regulates = all_parents_subont.parents(k, relations=['RO:0002211'])
            negatively_regulates = all_parents_subont.parents(k, relations=['RO:0002212'])
            positively_regulates = all_parents_subont.parents(k, relations=['RO:0002213'])

            definition = node.get('def')
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
                        defLinks = defLinks.replace("url:www", "http://www")
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
            definition = node.get("def")
            if definition is None:
                definition = ""

            print(node['id'])
            print(defText)
            print(defLinksProcessed)
            print(definition)

            newSubset = node.get('subset')
            if isinstance(newSubset, (list, tuple)):
                subset = newSubset
            else:
                if newSubset is not None:
                    subset.append(newSubset)

            # TODO: make this a generic section based on hte resourceDescriptor.yaml file.  need to have MODs add disease pages to their yaml stanzas


            alt_ids = node.get('alt_id')
            if alt_ids:
                if not isinstance(alt_ids, (list, tuple)):
                    alt_ids = [alt_ids]
            else:
                alt_ids = []

            dict_to_append = {
                'o_genes': [],
                'o_species': [],
                'name': node.get('label'),
                'o_synonyms': syns,
                'name_key': node.get('label'),
                'oid': node['id'],
                'definition': definition,
                'isas': isasWithoutNames,
                'partofs': partofsWithoutNames,
                'regulates': regulates,
                'negatively_regulates': negatively_regulates,
                'positively_regulates': positively_regulates,
                'is_obsolete': is_obsolete,
                'subset': subset,
                'xrefs': xrefs,
                #TODO: fix links to not be passed for each ontology load.
                'rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=All&x=1&acc_id='+node['id']+'#annot',
                'rgd_all_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=All&x=1&acc_id=' + node['id'] + '#annot',
                'rat_only_rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=Rat&x=1&acc_id=' +node['id'] + '#annot',
                'human_only_rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=Human&x=1&acc_id=' +node['id'] + '#annot',
                'mgi_link': 'http://www.informatics.jax.org/disease/'+node['id'],
                'wormbase_link': 'http://www.wormbase.org/resources/disease/'+node['id'],
                'flybase_link': 'http://flybase.org/cgi-bin/cvreport.html?id='+node['id'],
                'zfin_link': 'https://zfin.org/'+node['id'],
                'oUrl': "http://www.disease-ontology.org/?id=" + node['id'],
                'oPrefix': prefix,
                'crossReferences': xref_urls,
                'defText': defText,
                'defLinksProcessed': defLinksProcessed,
                'oboFile': prefix,
                'href': 'http://amigo.geneontology.org/amigo/term/' + node['id'],
                'category': 'go',
                'o_type': node.get('namespace'),
                'alt_ids': alt_ids,
            }
            node = {**node, **dict_to_append}
            ont.graph.node[node["id"]] = node

        # if testObject.using_test_data() is True:
        #     filtered_dict = []
        #     for entry in list_to_return:
        #         if testObject.check_for_test_ontology_entry(entry['id']) is True:
        #             filtered_dict.append(entry)
        #         else:
        #             continue
        #     return filtered_dict
        # else:
        # return
        return ont

    #TODO: add these to resourceDescriptors.yaml and remove hardcoding.
    def get_complete_url_ont (self, local_id, global_id):

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
