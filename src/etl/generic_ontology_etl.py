import logging, multiprocessing

from etl import ETL
from etl.helpers import ETLHelper, OBOHelper
from files import TXTFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor

logger = logging.getLogger(__name__)


class GenericOntologyETL(ETL):

    generic_ontology_term_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        //Create the Term node and set properties. primaryKey is required.
        MERGE (g:%sTerm:Ontology:AnatomyOntology {primaryKey:row.oid})
            ON CREATE SET g.definition = row.definition,
                g.type = row.o_type,
                g.href = row.href,
                g.name = row.name,
                g.nameKey = row.name_key,
                g.is_obsolete = row.is_obsolete,
                g.href = row.href,
                g.display_synonym = row.display_synonym
        MERGE (g)-[gccg:IS_A_PART_OF_SELF_CLOSURE]->(g)
        """

    generic_ontology_synonyms_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:%sTerm:Ontology {primaryKey:row.oid})
            MERGE (syn:Synonym:Identifier {primaryKey:row.syn})
                    SET syn.name = row.syn
            MERGE (g)-[aka:ALSO_KNOWN_AS]->(syn)
        """

    generic_ontology_isas_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:%sTerm:Ontology {primaryKey:row.oid})
            MERGE (g2:%sTerm:Ontology {primaryKey:row.isa})
            MERGE (g)-[aka:IS_A]->(g2)
        """

    generic_ontology_partofs_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:%sTerm:Ontology {primaryKey:row.oid})
            MERGE (g2:%sTerm:Ontology {primaryKey:row.partof})
            MERGE (g)-[aka:PART_OF]->(g2)
        """

    generic_ontology_regulates_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g1:%sTerm {primaryKey:row.primary_id})
            MERGE (g2:%sTerm:Ontology {primaryKey:row.primary_id2})
            MERGE (g1)-[aka:REGULATES]->(g2) """

    generic_ontology_negregs_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g1:%sTerm {primaryKey:row.primary_id})
            MERGE (g2:%sTerm:Ontology {primaryKey:row.primary_id2})
            MERGE (g1)-[aka:NEGATIVELY_REGULATES]->(g2) """

    generic_ontology_posregs_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g1:%sTerm {primaryKey:row.primary_id})
            MERGE (g2:%sTerm:Ontology {primaryKey:row.primary_id2})
            MERGE (g1)-[aka:POSITIVELY_REGULATES]->(g2) """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []
        
        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        for thread in thread_pool:
            thread.join()
  
    def _process_sub_type(self, sub_type):
        logger.info("Loading Generic Ontology Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        
        logger.info("Finished Loading Generic Ontology Data: %s" % sub_type.get_data_provider())

        # This order is the same as the lists yielded from the get_generators function.    
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        ont_type = sub_type.get_data_provider()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [GenericOntologyETL.generic_ontology_term_template, 600000, "generic_ontology_term_" + ont_type + ".csv", ont_type],
            [GenericOntologyETL.generic_ontology_isas_template, commit_size, "generic_ontology_isas_" + ont_type + ".csv", ont_type, ont_type],
            [GenericOntologyETL.generic_ontology_partofs_template, commit_size, "generic_ontology_partofs_" + ont_type + ".csv", ont_type, ont_type],
            [GenericOntologyETL.generic_ontology_negregs_template, commit_size, "generic_ontology_negregs_" + ont_type + ".csv", ont_type, ont_type],
            [GenericOntologyETL.generic_ontology_posregs_template, commit_size, "generic_ontology_posregs_" + ont_type + ".csv", ont_type, ont_type],
            [GenericOntologyETL.generic_ontology_regulates_template, commit_size, "generic_ontology_regulates_" + ont_type + ".csv", ont_type, ont_type],
            [GenericOntologyETL.generic_ontology_synonyms_template, 400000,
             "generic_ontology_synonyms_" + ont_type + ".csv", ont_type],
        ]

        # Obtain the generator
        generators = self.get_generators(filepath, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, filepath, batch_size):

        o_data = TXTFile(filepath).get_data()
        parsed_line = OBOHelper.parseOBO(o_data)

        counter = 0
        terms = []
        syns = []
        isas = []
        partofs = []
        negregs = []
        posregs = []
        regs = []

        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.

            counter += 1
            o_syns = line.get('synonym')
            defText = None
            definition = ""
            is_obsolete = "false"
            syn = ""
            ident = line['id']
            prefix = ident.split(":")[0]

            if o_syns is not None:
                if isinstance(o_syns, (list, tuple)):
                    for syn in o_syns:
                        syn = syn.split("\"")[1].strip()
                        syns_dict_to_append = {
                            'oid' : ident,
                            'syn' : syn
                        }
                        syns.append(syns_dict_to_append) # Synonyms appended here.
                else:
                    syn = o_syns.split("\"")[1].strip()
                    syns_dict_to_append = {
                            'oid' : ident,
                            'syn' : syn
                        }
                    syns.append(syns_dict_to_append) # Synonyms appended here.
            display_synonym = line.get('property_value')
            if display_synonym is not None:
                if isinstance(display_synonym, (list, tuple)):
                    display_synonym = display_synonym
                else:
                    if "DISPLAY_SYNONYM" in display_synonym:
                        display_synonym = display_synonym.split("\"")[1].strip()
                    else:
                        display_synonym = ""

            # is_a processing
            o_is_as = line.get('is_a')
            if o_is_as is not None:
                if isinstance(o_is_as, (list, tuple)):
                    for isa in o_is_as:
                        isaWithoutName = isa.split("!")[0].strip()
                        isas_dict_to_append ={
                            'oid' : ident,
                            'isa' : isaWithoutName
                        }
                        isas.append(isas_dict_to_append)
                else:
                    isaWithoutName = o_is_as.split("!")[0].strip()
                    isas_dict_to_append ={
                            'oid' : ident,
                            'isa' : isaWithoutName
                        }
                    isas.append(isas_dict_to_append)

            # part_of processing
            o_part_of = line.get('part_of')
            if o_part_of is not None:
                if isinstance(o_part_of, (list, tuple)):
                    for po in o_part_of:
                        poWithoutName = po.split("!")[0].strip()
                        partof_dict_to_append = {
                            'oid' : ident,
                            'partof' : poWithoutName
                        }
                        partofs.append(partof_dict_to_append)
                else:
                    poWithoutName = po.split("!")[0].strip()
                    partof_dict_to_append = {
                        'oid' : ident,
                        'partof' : poWithoutName
                        }
                    partofs.append(partof_dict_to_append)

            o_posreg = line.get('positively_regulates')
            if o_posreg is not None:
                if isinstance(o_posreg, (list, tuple)):
                    for pr in o_posreg:
                        prWithoutName = pr.split("!")[0].strip()
                        partof_dict_to_append = {
                            'oid' : ident,
                            'positevly_regulates' : prWithoutName
                        }
                        posregs.append(positively_regulates_dict_to_append)
                else:
                    prWithoutName = pr.split("!")[0].strip()
                    positively_regulates_dict_to_append = {
                        'oid' : ident,
                        'positevly_regulates' : prWithoutName
                        }
                    posregs.append(positively_regulates_dict_to_append)

            o_negreg = line.get('negatively_regulates')
            if o_negreg is not None:
                if isinstance(o_negreg, (list, tuple)):
                    for nr in o_negreg:
                        nrWithoutName = nr.split("!")[0].strip()
                        negatively_regulates_dict_to_append = {
                            'oid' : ident,
                            'negatively_regulates' : nrWithoutName
                        }
                        negregs.append(negatively_regulates_dict_to_append)
                else:
                    nrWithoutName = nr.split("!")[0].strip()
                    negatively_regulates_dict_to_append = {
                        'oid' : ident,
                        'negatively_regulates' : nrWithoutName
                        }
                    negregs.append(negatively_regulates_dict_to_append)

            o_reg = line.get('negatively_regulates')
            if o_reg is not None:
                if isinstance(o_reg, (list, tuple)):
                    for reg in o_reg:
                        rWithoutName = reg.split("!")[0].strip()
                        negatively_regulates_dict_to_append = {
                            'oid' : ident,
                            'negatively_regulates' : rWithoutName
                        }
                        regs.append(negatively_regulates_dict_to_append)
                else:
                    rWithoutName = po.split("!")[0].strip()
                    negatively_regulates_dict_to_append = {
                        'oid' : ident,
                        'negatively_regulates' : rWithoutName
                        }
                    regs.append(negatively_regulates_dict_to_append)


            definition = line.get('def')
            if definition is None:
                definition = ""
            else:
                if "\\\"" in definition: # Looking to remove instances of \" in the definition string.
                    definition = definition.replace('\\\"', '\"') # Replace them with just a single "
                else:
                    definition = defText
            if definition is None:
                definition = ""

            is_obsolete = line.get('is_obsolete')
            if is_obsolete is None:
                is_obsolete = "false"

            if ident is None or ident == '':
                logger.warn("Missing oid.")
            else:
                term_dict_to_append = {
                    'name': line.get('name'),
                    'name_key': line.get('name'),
                    'oid': ident,
                    'definition': definition,
                    'is_obsolete': is_obsolete,
                    'oPrefix': prefix,
                    'defText': defText,
                    'oboFile': prefix,
                    'o_type': line.get('namespace'),
                    'display_synonym': display_synonym
                }

                terms.append(term_dict_to_append)

            # Establishes the number of genes to yield (return) at a time.
            if counter == batch_size:
                counter = 0
                yield [terms, isas, partofs, negregs, posregs, regs, syns]
                terms = []
                syns = []
                isas = []
                partofs = []

        if counter > 0:
            yield [terms, isas, partofs, negregs, posregs, regs, syns]