import logging, multiprocessing

from etl import ETL
from etl.helpers import ETLHelper, OBOHelper
from files import JSONFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor

logger = logging.getLogger(__name__)


class GenericOntologyETL(ETL):
    generic_ontology_term_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

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
        MERGE (g)-[gccg:IS_A_PART_OF_CLOSURE]->(g)
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

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):
        logger.info("Loading Generic Ontology Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        ont_type = sub_type.get_data_provider()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [GenericOntologyETL.generic_ontology_term_template, 600000, "generic_ontology_term_" + ont_type + ".csv",
             ont_type],
            [GenericOntologyETL.generic_ontology_isas_template, commit_size,
             "generic_ontology_isas_" + ont_type + ".csv", ont_type, ont_type],
            [GenericOntologyETL.generic_ontology_partofs_template, commit_size,
             "generic_ontology_partofs_" + ont_type + ".csv", ont_type, ont_type],
            [GenericOntologyETL.generic_ontology_synonyms_template, 400000,
             "generic_ontology_synonyms_" + ont_type + ".csv", ont_type],
        ]

        # Obtain the generator
        generators = self.get_generators(filepath, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        logger.info("Finished Loading Generic Ontology Data: %s" % sub_type.get_data_provider())

    def get_generators(self, filepath, batch_size):
        o_data = JSON.File(filepath).get_data()



            if counter == batch_size:
                counter = 0
                yield [terms, isas, partofs, syns]
                terms = []
                syns = []
                isas = []
                partofs = []

        if counter > 0:
            yield [terms, isas, partofs, syns]
