import logging

from ontobio import OntologyFactory

from etl import ETL
from etl.helpers import ETLHelper
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)

class DOETL(ETL):


    do_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        //Create the DOTerm node and set properties. primaryKey is required.
        MERGE (doterm:DOTerm:Ontology {primaryKey:row.oid})
            SET doterm.name = row.name,
             doterm.nameKey = row.name_key,
             doterm.definition = row.definition,
             doterm.defLinks = apoc.convert.fromJsonList(row.defLinksProcessed),
             doterm.is_obsolete = row.is_obsolete,
             doterm.subset = row.subset,
             doterm.doDisplayId = row.oid,
             doterm.doUrl = row.oUrl,
             doterm.doPrefix = "DOID",
             doterm.doId = row.oid,
             doterm.rgdLink = row.rgd_link,
             doterm.ratOnlyRgdLink = row.rat_only_rgd_link,
             doterm.humanOnlyRgdLink = row.human_only_rgd_link,
             doterm.mgiLink = row.mgi_link,
             doterm.zfinLink = row.zfin_link,
             doterm.flybaseLink = row.flybase_link,
             doterm.wormbaseLink = row.wormbase_link,
             doterm.sgdLink = row.sgd_link 
             
            MERGE (doterm)-[ggcg:IS_A_PART_OF_SELF_CLOSURE]->(doterm)"""
    
    doterm_synonyms_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (d:DOTerm {primaryKey:row.primary_id})
            
            MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                SET syn.name = row.synonym
            MERGE (d)-[aka2:ALSO_KNOWN_AS]->(syn) """
            
    doterm_isas_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (d1:DOTerm {primaryKey:row.primary_id})
            MERGE (d2:DOTerm:Ontology {primaryKey:row.primary_id2})
            MERGE (d1)-[aka:IS_A]->(d2) """
            
    xrefs_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:DOTerm {primaryKey:row.oid}) """ + ETLHelper.get_cypher_xref_text()
            
            
    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        filepath = self.data_type_config.get_single_filepath()

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()
        
        generators = self.get_generators(filepath, batch_size)

        query_list = [
            [DOETL.do_query_template, commit_size, "do_term_data.csv"],
            [DOETL.doterm_isas_template, commit_size, "do_isas_data.csv"],
            [DOETL.doterm_synonyms_template, commit_size, "do_synonyms_data.csv"],
            [DOETL.xrefs_template, commit_size, "do_xrefs_data.csv"],
        ]
        
        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, filepath, batch_size):
        
        ont = OntologyFactory().create(filepath)
        parsed_line = ont.graph.copy().node
        
        do_term_list = []
        do_isas_list = []
        do_synonyms_list = []
        xrefs = []
        counter = 0
        
        for k, line in parsed_line.items():  # Convert parsed obo term into a schema-friendly AGR dictionary.
            counter = counter + 1
            node = ont.graph.node[k]
            if len(node) == 0:
                continue

            ### Switching id to curie form and saving URI in "uri" - might wildly break things later on???
            node["uri"] = node["id"]
            node["id"] = k

            syns = []

            local_id = None
            defLinksUnprocessed = []
            defLinksProcessed = []
            subset = []
            definition = ""
            is_obsolete = "false"
            ident = k
            prefix = ident.split(":")[0]

            if "meta" in node:
                if "synonyms" in node["meta"]:
                    syns = [s["val"] for s in node["meta"]["synonyms"]]
                    for synonym in syns:
                        doSynonym = {
                            "primary_id": k,
                            "synonym": synonym
                        }
                        do_synonyms_list.append(doSynonym)
                if "xrefs" in node["meta"]:

                    o_xrefs = node["meta"].get('xrefs')
                    if o_xrefs is not None:
                        for xrefIdDict in o_xrefs:
                            xrefId = xrefIdDict["val"]
                            if ":" in xrefId:
                                local_id = xrefId.split(":")[1].strip()
                                prefix = xrefId.split(":")[0].strip()
                                complete_url = ETLHelper.get_complete_url_ont(local_id, xrefId)
                                generated_xref = ETLHelper.get_xref_dict(local_id, prefix, "ontology_provided_cross_reference", "ontology_provided_cross_reference", xrefId, complete_url, xrefId + "ontology_provided_cross_reference")
                                generated_xref["oid"] = ident
                                xrefs.append(generated_xref)
                        else:
                            if ":" in o_xrefs:
                                local_id = o_xrefs.split(":")[1].strip()
                                prefix = o_xrefs.split(":")[0].strip()
                                complete_url = ETLHelper.get_complete_url_ont(local_id, o_xrefs)
                                generated_xref = ETLHelper.get_xref_dict(local_id, prefix, "ontology_provided_cross_reference", "ontology_provided_cross_reference", o_xrefs, complete_url, o_xrefs)
                                generated_xref["oid"] = ident
                                xrefs.append(generated_xref)
                if node["meta"].get('is_obsolete'):
                    is_obsolete = "true"
                elif node["meta"].get('deprecated'):
                    is_obsolete = "true"
                if "definition" in node["meta"]:
                    definition = node["meta"]["definition"]["val"]
                    defLinksUnprocessed = node["meta"]["definition"]["xrefs"]
                if "subsets" in node["meta"]:
                    newSubset = node['meta'].get('subsets')
                    if isinstance(newSubset, (list, tuple)):
                        subset = newSubset
                    else:
                        if newSubset is not None:
                            subset.append(newSubset)
                if len(subset) > 1:
                    converted_subsets = []
                    for s in subset:
                        if "#" in s:
                            s = s.split("#")[-1]
                        converted_subsets.append(s)
                    subset = converted_subsets

            all_parents = ont.parents(k)
            all_parents.append(k)
            all_parents_subont = ont.subontology(all_parents) # Improves performance when traversing relations

            isasWithoutNames = all_parents_subont.parents(k, relations=['subClassOf'])

            for item in isasWithoutNames:
                dictionary = {
                    "primary_id": k,
                    "primary_id2": item
                }
                do_isas_list.append(dictionary)

            defLinksProcessed = []
            defLinks = ""
            if definition is None:
                definition = ""
            else:
                #definition = definition.replace('\n', ' ') # Remove new lines that cause this to split across two lines in the file
                #definition = definition.replace('  ', ' ') # Remove any extra double space that might have been introduces in the last replace
                if definition is not None and "\"" in definition:
                    split_definition = definition.split("\"")
                    if len(split_definition) > 1:
                        if len(split_definition) > 2 and "[" in split_definition[2].strip():
                            defLinks = split_definition[2].strip()
                            defLinksUnprocessed.append(defLinks.rstrip("]").replace("[", ""))


            for dl in defLinksUnprocessed:
                dl = dl.replace("url:www", "http://www")
                dl = dl.replace("url:", "")
                dl = dl.replace("URL:", "")
                dl = dl.replace("\\:", ":")

                if "," in dl:
                    dl = dl.split(",")
                    for link in dl:
                        if link.strip().startswith("http"):
                            defLinksProcessed.append(link)
                # elif "." in dl:
                #     dl = dl.split(".")
                #     for link in dl:
                #         if link.strip().startswith("http"):
                #             defLinksProcessed.append(link)
                else:
                    if dl.strip().startswith("http"):
                        defLinksProcessed.append(dl)

            # TODO: make this a generic section based on hte resourceDescriptor.yaml file.  need to have MODs add disease pages to their yaml stanzas


            alt_ids = node.get('alt_id')
            if alt_ids:
                if not isinstance(alt_ids, (list, tuple)):
                    alt_ids = [alt_ids]
            else:
                alt_ids = []
            
            dict_to_append = {
                'oid': node['id'],
                'name': node.get('label'),
                'name_key': node.get('label'),
                'definition': definition,
                'defLinksProcessed': defLinksProcessed,
                'is_obsolete': is_obsolete,
                'subset': subset,
                'oUrl': "http://www.disease-ontology.org/?id=" + node['id'],
                'rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=All&x=1&acc_id='+node['id']+'#annot',
                'rat_only_rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=Rat&x=1&acc_id=' +node['id'] + '#annot',
                'human_only_rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?species=Human&x=1&acc_id=' +node['id'] + '#annot',
                'mgi_link': 'http://www.informatics.jax.org/disease/'+node['id'],
                'zfin_link': 'https://zfin.org/'+node['id'],
                'flybase_link': 'http://flybase.org/cgi-bin/cvreport.html?id='+node['id'],
                'wormbase_link': 'http://www.wormbase.org/resources/disease/'+node['id'],
                'sgd_link': 'https://yeastgenome.org/disease/'+node['id'],

            }
            
            do_term_list.append(dict_to_append)
            

            if counter == batch_size:
                yield [do_term_list, do_isas_list, do_synonyms_list, xrefs]
                do_term_list = []
                do_isas_list = []
                do_synonyms_list = []
                xrefs = []
                counter = 0

        if counter > 0:
            yield [do_term_list, do_isas_list, do_synonyms_list, xrefs]
            
            