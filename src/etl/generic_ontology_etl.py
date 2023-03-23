
"""Generic Ontology ETL."""

import logging
import multiprocessing
import re

from etl import ETL
from etl.helpers import OBOHelper
from files import TXTFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class GenericOntologyETL(ETL):
    """Generic Ontology ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    generic_ontology_term_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                //Create the Term node and set properties. primaryKey is required.
                MERGE (g:%sTerm:Ontology {primaryKey:row.oid})
                    ON CREATE SET g.definition = row.definition,
                        g.type = row.o_type,
                        g.href = row.href,
                        g.name = row.name,
                        g.nameKey = row.name_key,
                        g.isObsolete = row.is_obsolete,
                        g.href = row.href,
                        g.displaySynonym = row.display_synonym,
                        g.subsets = apoc.convert.fromJsonList(row.subsets)
                CREATE (g)-[gccg:IS_A_PART_OF_CLOSURE]->(g)
            }
        IN TRANSACTIONS of %s ROWS"""

    generic_ontology_synonyms_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (g:%sTerm {primaryKey:row.oid})
                MERGE (syn:Synonym:Identifier {primaryKey:row.syn})
                        SET syn.name = row.syn
                MERGE (g)-[aka:ALSO_KNOWN_AS]->(syn)
            }
        IN TRANSACTIONS of %s ROWS"""

    generic_ontology_isas_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (g:%sTerm {primaryKey:row.oid})
                MATCH (g2:%sTerm {primaryKey:row.isa})
                CREATE (g)-[aka:IS_A]->(g2)
            }
        IN TRANSACTIONS of %s ROWS"""

    generic_ontology_partofs_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (g:%sTerm {primaryKey:row.oid})
                MATCH (g2:%sTerm {primaryKey:row.partof})
                CREATE (g)-[aka:PART_OF]->(g2)
            }
        IN TRANSACTIONS of %s ROWS"""

    generic_ontology_altids_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (got:%sTerm {primaryKey:row.primary_id})
                MERGE(sec:SecondaryId:Identifier {primaryKey:row.secondary_id})
                CREATE (got)-[aka2:ALSO_KNOWN_AS]->(sec)
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):
        self.logger.info("Loading Generic Ontology Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        # commit_size = self.data_type_config.get_neo4j_commit_size()
        commit_size = 100000000
        batch_size = self.data_type_config.get_generator_batch_size()

        ont_type = sub_type.get_data_provider()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.generic_ontology_term_query_template,
             "generic_ontology_term_" + ont_type + ".csv", ont_type, commit_size],
            [self.generic_ontology_isas_query_template, 
             "generic_ontology_isas_" + ont_type + ".csv", ont_type, ont_type, commit_size],
            [self.generic_ontology_partofs_query_template,
             "generic_ontology_partofs_" + ont_type + ".csv", ont_type, ont_type, commit_size],
            [self.generic_ontology_synonyms_query_template,
             "generic_ontology_synonyms_" + ont_type + ".csv", ont_type, commit_size],
            [self.generic_ontology_altids_query_template, 
             "generic_ontology_altids_" + ont_type + ".csv", ont_type, commit_size],
        ]

        # Obtain the generator
        generators = self.get_generators(filepath, batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("GenOnt-{}: ".format(sub_type.get_data_provider()))
        self.logger.info("Finished Loading Generic Ontology Data: %s", sub_type.get_data_provider())

    def get_generators(self, filepath, batch_size):  # noqa
        """Get Generators."""

        OBOHelper.add_metadata_to_neo(filepath)
        o_data = TXTFile(filepath).get_data()
        parsed_line = OBOHelper.parse_obo(o_data)

        counter = 0

        terms = []
        syns = []
        isas = []
        partofs = []
        subsets = []
        altids = []

        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.

            counter += 1
            o_syns = line.get('synonym')
            ident = line['id'].strip()
            prefix = ident.split(":")[0]
            display_synonym = ""
            o_altids = line.get('alt_id')

            if o_altids is not None:
                if isinstance(o_altids, (list, tuple)):
                    for altid in o_altids:
                        alt_dict_to_append = {
                            'primary_id': ident,
                            'secondary_id': altid
                        }
                        altids.append(alt_dict_to_append)
                else:
                    alt_dict_to_append = {
                        'primary_id': ident,
                        'secondary_id': o_altids
                    }
                    altids.append(alt_dict_to_append)

            if o_syns is not None:
                if isinstance(o_syns, (list, tuple)):
                    for syn in o_syns:

                        synsplit = re.split(r'(?<!\\)"', syn)
                        syns_dict_to_append = {
                            'oid': ident,
                            'syn': synsplit[1].replace('\\"', '""')
                        }
                        syns.append(syns_dict_to_append)  # Synonyms appended here.
                        if "DISPLAY_SYNONYM" in syn:
                            display_synonym = synsplit[1].replace('"', '""')
                else:
                    synsplit = re.split(r'(?<!\\)"', o_syns)
                    syns_dict_to_append = {
                        'oid': ident,
                        'syn': synsplit[1].replace('\"', '""')
                    }
                    syns.append(syns_dict_to_append)  # Synonyms appended here.
                    if "DISPLAY_SYNONYM" in o_syns:
                        display_synonym = synsplit[1].replace('\"', '""')
            # subset
            new_subset = line.get('subset')
            subsets.append(new_subset)

            # is_a processing
            o_is_as = line.get('is_a')
            if o_is_as is not None:
                if isinstance(o_is_as, (list, tuple)):
                    for isa in o_is_as:
                        if 'gci_filler=' not in isa:
                            isa_without_name = isa.split(' ')[0].strip()
                            isas_dict_to_append = {
                                'oid': ident,
                                'isa': isa_without_name}
                            isas.append(isas_dict_to_append)
                else:
                    if 'gci_filler=' not in o_is_as:
                        isa_without_name = o_is_as.split(' ')[0].strip()
                        isas_dict_to_append = {'oid': ident,
                                               'isa': isa_without_name}
                        isas.append(isas_dict_to_append)

            # part_of processing
            relations = line.get('relationship')
            if relations is not None:
                if isinstance(relations, (list, tuple)):
                    for partof in relations:
                        if 'gci_filler=' not in partof:
                            relationship_descriptors = partof.split(' ')
                            o_part_of = relationship_descriptors[0]
                            if o_part_of == 'part_of':
                                partof_dict_to_append = {
                                    'oid': ident,
                                    'partof': relationship_descriptors[1]
                                }
                                partofs.append(partof_dict_to_append)
                else:
                    if 'gci_filler=' not in relations:
                        relationship_descriptors = relations.split(' ')
                        o_part_of = relationship_descriptors[0]
                        if o_part_of == 'part_of':
                            partof_dict_to_append = {
                                'oid': ident,
                                'partof': relationship_descriptors[1]}
                            partofs.append(partof_dict_to_append)

            definition = line.get('def')
            if definition is None:
                definition = ""
            else:
                # Looking to remove instances of \" in the definition string.
                if "\\\"" in definition:
                    # Replace them with just a single "
                    definition = definition.replace('\\\"', '\"')

            if definition is None:
                definition = ""

            is_obsolete = line.get('is_obsolete')
            if is_obsolete is None:
                is_obsolete = "false"

            if ident is None or ident == '':
                self.logger.warning("Missing oid.")
            else:
                term_dict_to_append = {
                    'name': line.get('name'),
                    'name_key': line.get('name'),
                    'oid': ident,
                    'definition': definition,
                    'is_obsolete': is_obsolete,
                    'oPrefix': prefix,
                    'oboFile': prefix,
                    'o_type': line.get('namespace'),
                    'display_synonym': display_synonym
                }

                terms.append(term_dict_to_append)

            # Establishes the number of genes to yield (return) at a time.
            if counter == batch_size:
                counter = 0
                yield [terms, isas, partofs, syns, altids]
                terms = []
                syns = []
                isas = []
                partofs = []

        if counter > 0:
            yield [terms, isas, partofs, syns, altids]
