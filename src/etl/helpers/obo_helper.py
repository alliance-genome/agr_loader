"""OBO Helper."""

import logging
import json
from collections import defaultdict

from ontobio import OntologyFactory
from .etl_helper import ETLHelper
from .neo4j_helper import Neo4jHelper


class OBOHelper():
    """OBO Helper."""

    logger = logging.getLogger(__name__)
    etlh = ETLHelper()

    @staticmethod
    def add_metadata_to_neo(filepath):
        """Get header information and place it, adds node into Neo."""
        header = OBOHelper.get_header(filepath)
        header["primaryKey"] = header["ontology"]
        header["date"] = ETLHelper.check_date_format(header["date"])
        fields = []
        for k in header:
            fields.append(k + ": " + json.dumps(header[k]))
        if not header["date"]:
            OBOHelper.logger.warning("Problem with getting date for : {}".format(header["ontology"]))
        Neo4jHelper().run_single_query_no_return("CREATE (o:OntologyFileMetadata {" + ",".join(fields) + "})")

    @staticmethod
    def get_header(filepath):
        """Retrieve header information into dictionary."""
        header = defaultdict(list)
        with open(filepath, 'r') as f:
            for line in f:
                if not line.strip() or line[0] == "#":
                    break
                [k, v] = line.rstrip().split(": ", 1)
                camel_k = ''.join(x.capitalize() or '-' for x in k.split('-'))
                camel_k = camel_k[0].lower() + camel_k[1:]
                header[camel_k].append(str(v.replace('\"', "'")))

        for k in header:
            v = header[k]
            if len(v) == 1:
                header[k] = v[0]

        return header

    def get_data(self, filepath):  # noqa
        """Get Data."""
        ont = OntologyFactory().create(filepath)

        parsed_line = ont.graph.copy().node

        # Convert parsed obo term into a schema-friendly AGR dictionary.
        for key in parsed_line.items():
            node = ont.graph.node[key]
            if len(node) == 0:
                continue

            # Switching id to curie form and saving URI in "uri"
            # might wildly break things later on???
            node["uri"] = node["id"]
            node["id"] = key

            syns = []
            # So code commented out with NU: at start means it is Not Used.
            # NU: xrefs = []
            # NU: xref_urls = []

            # NU: def_links_unprocessed = []
            # NU: def_links_processed = []
            subset = []
            definition = ""
            namespace = ""
            is_obsolete = "false"
            # NU:ident = key

            if "meta" in node:
                if "synonyms" in node["meta"]:
                    syns = [s["val"] for s in node["meta"]["synonyms"]]
                # NU: leave in call commented out in case it is used at a later time
                # if "xrefs" in node["meta"]:
                #     o_xrefs = node["meta"].get('xrefs')
                #     self.ortho_xrefs(o_xrefs, ident, xref_urls)
                if node["meta"].get('is_obsolete'):
                    is_obsolete = "true"
                elif node["meta"].get('deprecated'):
                    is_obsolete = "true"
                if "definition" in node["meta"]:
                    definition = node["meta"]["definition"]["val"]
                    # NU: def_links_unprocessed = node["meta"]["definition"]["xrefs"]
                if "subsets" in node["meta"]:
                    new_subset = node['meta'].get('subsets')
                    if isinstance(new_subset, (list, tuple)):
                        subset = new_subset
                    else:
                        if new_subset is not None:
                            subset.append(new_subset)
                if len(subset) > 1:
                    converted_subsets = []
                    for subset_str in subset:
                        if "#" in subset_str:
                            subset_str = subset_str.split("#")[-1]
                        converted_subsets.append(subset_str)
                    subset = converted_subsets
                if "basicPropertyValues" in node['meta']:
                    for bpv in node['meta']['basicPropertyValues']:
                        if bpv.get('pred') == 'OIO:hasOBONamespace':
                            namespace = bpv.get('val')
                            break

            all_parents = ont.parents(key)
            all_parents.append(key)

            # Improves performance when traversing relations
            all_parents_subont = ont.subontology(all_parents)

            isas_without_names = all_parents_subont.parents(key, relations=['subClassOf'])
            partofs_without_names = all_parents_subont.parents(key, relations=['BFO:0000050'])
            regulates = all_parents_subont.parents(key, relations=['RO:0002211'])
            negatively_regulates = all_parents_subont.parents(key, relations=['RO:0002212'])
            positively_regulates = all_parents_subont.parents(key, relations=['RO:0002213'])

            # NU: def_links_unprocessed = []
            # def_links = ""
            if definition is None:
                definition = ""
            # else:
            #     if definition is not None and "\"" in definition:
            #         split_definition = definition.split("\"")
            #         if len(split_definition) > 1:
            #             if len(split_definition) > 2 and "[" in split_definition[2].strip():
            #                 def_links = split_definition[2].strip()
            #                 def_links_unprocessed.append(def_links.rstrip("]").replace("[", ""))

            # NU: def_links_processed not used later, it is commented out.
            # for def_link_str in def_links_unprocessed:
            #     def_link_str = def_link_str.replace("url:www", "http://www")
            #     def_link_str = def_link_str.replace("url:", "")
            #     def_link_str = def_link_str.replace("URL:", "")
            #     def_link_str = def_link_str.replace("\\:", ":")

            #     if "," in def_link_str:
            #         def_links = def_link_str.split(",")
            #         for link in def_links:
            #             if link.strip().startswith("http"):
            #                 def_links_processed.append(link)
            #     else:
            #         if def_link_str.strip().startswith("http"):
            #             def_links_processed.append(def_link_str)

            # NU: alt_ids = node.get('alt_id')
            # if alt_ids:
            #    if not isinstance(alt_ids, (list, tuple)):
            #        alt_ids = [alt_ids]
            # else:
            #    alt_ids = []

            dict_to_append = {

                'o_type': namespace,
                'name': node.get('label'),
                'href': 'http://amigo.geneontology.org/amigo/term/' + node['id'],
                'name_key': node.get('label'),
                'oid': node['id'],
                'definition': definition,
                'is_obsolete': is_obsolete,
                'subset': subset,
                'o_synonyms': syns,
                'isas': isas_without_names,
                'partofs': partofs_without_names,
                'regulates': regulates,
                'negatively_regulates': negatively_regulates,
                'positively_regulates': positively_regulates,

                # This data might be needed for gene descriptions
                # Maybe should be turned into a different method in order
                # to keep the go do dict's smaller
                # 'o_genes': [],
                # 'o_species': [],
                # 'xrefs': xrefs,
                # 'ontologyLabel': filepath,
                # TODO: fix links to not be passed for each ontology load.
                # 'rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html'\
                #              + '?species=All&x=1&acc_id='+node['id']+'#annot',
                # 'rgd_all_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?'\
                #                   + 'species=All&x=1&acc_id=' + node['id'] + '#annot',
                # 'rat_only_rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?'\
                #                       + 'species=Rat&x=1&acc_id=' +node['id'] + '#annot',
                # 'human_only_rgd_link': 'http://rgd.mcw.edu/rgdweb/ontology/annot.html?'\
                #                        + 'species=Human&x=1&acc_id=' +node['id'] + '#annot',
                # 'mgi_link': 'http://www.informatics.jax.org/disease/'+node['id'],
                # 'wormbase_link': 'http://www.wormbase.org/resources/disease/'+node['id'],
                # 'sgd_link': 'https://yeastgenome.org/disease/'+node['id'],
                # 'flybase_link': 'http://flybase.org/cgi-bin/cvreport.html?id='+node['id'],
                # 'zfin_link': 'https://zfin.org/'+node['id'],
                # 'oUrl': "http://www.disease-ontology.org/?id=" + node['id'],
                # 'oPrefix': prefix,
                # 'crossReferences': xref_urls,
                # 'defText': def_text,
                # 'defLinksProcessed': def_links_processed,
                # 'oboFile': prefix,
                # 'category': 'go',
                # 'alt_ids': alt_ids,
            }

            if node['id'] == 'GO:0099616':
                self.logger.debug(dict_to_append)

            node = {**node, **dict_to_append}
            ont.graph.node[node["id"]] = node

        return ont

    @staticmethod
    def process_line(line, o_dict, within_term):
        """Process Line."""
        if len(line.strip()) == 0:  # If the line is blank, reset withinTerm and kick it back.
            within_term = False
            return o_dict, within_term  # The o_dict should be fully populated at this point.

        if ":" in line:
            # Split the lines on the first ':'
            key, value = line.strip().split(':', 1)

            # Remove erroneous first character from the split.
            # TODO Typical whitespace removal doesn't work? Why?
            value = value[1:]
            if key in o_dict:
                # If it's an entry with a single string, turn it into a list.
                if isinstance(o_dict[key], str):
                    temp_value = o_dict[key]
                    o_dict[key] = [temp_value, value]
                # If it's already a list, append to it.
                elif isinstance(o_dict[key], list):
                    o_dict[key].append(value)
            # If it's the first time we're seeing this key-value, make a new entry.
            else:
                o_dict[key] = value
        else:
            OBOHelper.logger.info(line)
            OBOHelper.logger.info(o_dict)

        return o_dict, within_term

    @staticmethod
    def parse_obo(data):
        """Parse OBO."""
        ontology_data = []
        o_dict = {}
        within_term = False
        within_typedef = False

        # Ignores withinTypedef entries.
        for line in data:
            if '[Term]' in line:
                within_term = True

                # If o_dict has data (from pervious [Term]) add it to the list first.
                if o_dict:
                    ontology_data.append(o_dict)
                    o_dict = {}
                else:
                    continue
            elif '[Typedef]' in line:
                within_typedef = True  # Used for skipping data.
            else:
                if within_term is True:
                    o_dict, within_term = OBOHelper.process_line(line, o_dict, within_term)
                elif within_typedef is True:  # Skip Typedefs, look for empty line.
                    if len(line.strip()) == 0:
                        within_typedef = False  # Reset withinTypedef
        ontology_data.append(o_dict)

        return ontology_data
