"""DO ETL."""

import logging
import re
from ontobio import OntologyFactory

from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import OBOHelper
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class DOETL(ETL):
    """DO ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    do_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                //Create the DOTerm node and set properties. primaryKey is required.
                MERGE (doterm:DOTerm {primaryKey:row.oid})
                    SET doterm.name = row.name,
                    doterm :Ontology,
                    doterm.nameKey = row.name_key,
                    doterm.definition = row.definition,
                    doterm.defLinks = apoc.convert.fromJsonList(row.defLinksProcessed),
                    doterm.isObsolete = row.is_obsolete,
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

                    MERGE (doterm)-[ggcg:IS_A_PART_OF_CLOSURE]->(doterm)
            }
        IN TRANSACTIONS of %s ROWS"""

    doterm_synonyms_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (d:DOTerm {primaryKey:row.primary_id})

                MERGE (syn:Synonym:Identifier {primaryKey:row.synonym})
                    SET syn.name = row.synonym
                MERGE (d)-[aka2:ALSO_KNOWN_AS]->(syn)
            }
        IN TRANSACTIONS of %s ROWS"""

    doterm_isas_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (d1:DOTerm{primaryKey:row.primary_id})
                MATCH (d2:DOTerm {primaryKey:row.primary_id2})
                MERGE (d1)-[aka:IS_A]->(d2)
            }
        IN TRANSACTIONS of %s ROWS"""
    
    xrefs_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (o:DOTerm {primaryKey:row.oid}) 
                """ + ETLHelper.get_cypher_xref_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    doterm_alt_ids_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row
                
                MATCH (d:DOTerm {primaryKey:row.primary_id})
                MERGE (sec:SecondaryId:Identifier {primaryKey:row.secondary_id})
                MERGE (d)-[aka2:ALSO_KNOWN_AS]->(sec)
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        filepath = self.data_type_config.get_single_filepath()

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(filepath, batch_size)

        query_template_list = [
            [self.do_query_template, "do_term_data.csv", commit_size],
            [self.doterm_isas_query_template, "do_isas_data.csv", commit_size],
            [self.doterm_synonyms_query_template, "do_synonyms_data.csv", commit_size],
            [self.xrefs_query_template, "do_xrefs_data.csv", commit_size],
            [self.doterm_alt_ids_query_template, "do_alt_ids_data.csv", commit_size]
        ]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("DO-?: ")

    def get_generators(self, filepath, batch_size):  # noqa TODO:Needs splitting up really
        """Get Generators."""
        ont = OntologyFactory().create(filepath)
        parsed_line = ont.graph.copy().node
        OBOHelper.add_metadata_to_neo(filepath)

        do_term_list = []
        do_isas_list = []
        do_synonyms_list = []
        do_alt_ids_list = []
        xrefs = []
        counter = 0

        # Convert parsed obo term into a schema-friendly AGR dictionary.
        for key, line in parsed_line.items():
            counter = counter + 1
            node = ont.graph.node[key]
            if len(node) == 0:
                continue

            # Switching id to curie form and saving URI in "uri"
            # - might wildly break things later on???
            node["uri"] = node["id"]
            node["id"] = key

            syns = []

            def_links_unprocessed = []
            def_links_processed = []
            subset = []
            definition = ""
            is_obsolete = "false"
            ident = key

            if "meta" in node:
                if "synonyms" in node["meta"]:
                    syns = [s["val"] for s in node["meta"]["synonyms"]]
                    for synonym in syns:
                        do_synonym = {
                            "primary_id": key,
                            "synonym": synonym
                        }
                        do_synonyms_list.append(do_synonym)

                if "basicPropertyValues" in node["meta"]:
                    alt_ids = [s["val"] for s in node["meta"]["basicPropertyValues"]]
                    for alt_id in alt_ids:
                        if "DOID:" in alt_id:
                            secondary_id = {
                                "primary_id": key,
                                "secondary_id": alt_id
                            }
                            do_alt_ids_list.append(secondary_id)

                if "xrefs" in node["meta"]:
                    o_xrefs = node["meta"].get('xrefs')
                    self.ortho_xrefs(o_xrefs, ident, xrefs)

                if node["meta"].get('is_obsolete'):
                    is_obsolete = "true"
                elif node["meta"].get('deprecated'):
                    is_obsolete = "true"
                if "definition" in node["meta"]:
                    definition = node["meta"]["definition"]["val"]
                    def_links_unprocessed = node["meta"]["definition"]["xrefs"]
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

            all_parents = ont.parents(key)
            all_parents.append(key)

            # Improves performance when traversing relations
            all_parents_subont = ont.subontology(all_parents)
            isas_without_names = all_parents_subont.parents(key, relations=['subClassOf'])

            for item in isas_without_names:
                dictionary = {
                    "primary_id": key,
                    "primary_id2": item
                }

                do_isas_list.append(dictionary)

            def_links_processed = []
            def_links = ""
            if definition is None:
                definition = ""
            else:
                # Remove new lines that cause this to split across two lines in the file
                # definition = definition.replace('\n', ' ')

                # Remove any extra double space that might have been introduces in the last replace
                # definition = definition.replace('  ', ' ')

                if definition is not None and "\"" in definition:
                    split_definition = re.split(r'(?<!\\)"', definition)
                    if len(split_definition) > 1:
                        if len(split_definition) > 2 and "[" in split_definition[2].strip():
                            def_links = split_definition[2].strip()
                            def_links = def_links.rstrip("]").replace("[", "")
                            def_links_unprocessed.append(def_links)

            for def_link in def_links_unprocessed:
                def_link = def_link.replace("url:www", "http://www")
                def_link = def_link.replace("url:", "")
                def_link = def_link.replace("URL:", "")
                def_link = def_link.replace("\\:", ":")
                def_link = def_link.replace('\\', '')

                if "," in def_link:
                    def_link = def_link.split(",")
                    for link in def_link:
                        if link.strip().startswith("http"):
                            def_links_processed.append(link)
                else:
                    if def_link.strip().startswith("http"):
                        def_links_processed.append(def_link)

            # TODO: make this a generic section based on the resourceDescriptor.yaml file.
            # need to have MODs add disease pages to their yaml stanzas

            # NU: alt_ids = node.get('alt_id')
            # if alt_ids:
            #     if not isinstance(alt_ids, (list, tuple)):
            #         alt_ids = [alt_ids]
            # else:
            #     alt_ids = []

            # TODO: Need to add urls to resource Descriptis for SGD and MGI.
            # NOTE: MGI had one but has 'MGI:' at the end of the url not required here.
            dict_to_append = {
                'oid': node['id'],
                'name': node.get('label'),
                'name_key': node.get('label'),
                'definition': definition,
                'defLinksProcessed': def_links_processed,
                'is_obsolete': is_obsolete,
                'subset': subset,
                'oUrl': self.etlh.rdh2.return_url_from_key_value('DOID', node['id']),
                'rgd_link': self.etlh.rdh2.return_url_from_key_value('RGD', node['id'], 'disease/all'),
                'rat_only_rgd_link': self.etlh.rdh2.return_url_from_key_value('RGD', node['id'], 'disease/rat'),
                'human_only_rgd_link': self.etlh.rdh2.return_url_from_key_value('RGD', node['id'], 'disease/human'),
                'mgi_link': 'http://www.informatics.jax.org/disease/' + node['id'],
                'zfin_link': self.etlh.rdh2.return_url_from_key_value('ZFIN', node['id'], 'disease'),
                'flybase_link': self.etlh.rdh2.return_url_from_key_value('FB', node['id'], 'disease'),
                'wormbase_link': self.etlh.rdh2.return_url_from_key_value('WB', node['id'], 'disease'),
                'sgd_link': 'https://yeastgenome.org/disease/' + node['id']
            }

            do_term_list.append(dict_to_append)

            if counter == batch_size:
                yield [do_term_list, do_isas_list, do_synonyms_list, xrefs, do_alt_ids_list]
                do_term_list = []
                do_isas_list = []
                do_synonyms_list = []
                do_alt_ids_list = []
                xrefs = []
                counter = 0

        if counter > 0:
            yield [do_term_list, do_isas_list, do_synonyms_list, xrefs, do_alt_ids_list]
