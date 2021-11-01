"""ECOMAP ETL."""

import logging
import multiprocessing

from etl import ETL
from etl.helpers import OBOHelper
from files import TXTFile
from transactors import CSVTransactor
from transactors import Neo4jTransactor


class ECOMAPETL(ETL):
    """ECOMAP ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    eco_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (e:ECOTerm:Ontology {primaryKey: row.ecoId})
            SET e.displaySynonym = row.threeLetterCode"""

    def __init__(self, config):
        """Initialize object."""
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
        self.logger.info("Loading ECOMAP Ontology Data: %s", sub_type.get_data_provider())

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        filepath = sub_type.get_filepath()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_template_list = [
            [self.eco_query_template, commit_size,
             "ecomap_data_" + sub_type.get_data_provider() + ".csv"],
        ]

        # Obtain the generator
        generators = self.get_generators(filepath, batch_size)

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        self.error_messages("Ecomap-{}: ".format(sub_type.get_data_provider()))
        self.logger.info("Finished Loading ECOMAP Data: %s", sub_type.get_data_provider())

    def get_generators(self, filepath, batch_size):
        """Create Generator."""
        data = TXTFile(filepath).get_data()
        OBOHelper.add_metadata_to_neo(filepath)

        eco_maps = []

        for line in data:
            columns = line.split()
            if columns[0].startswith('#'):
                continue

            eco = {"ecoId": columns[1],
                   "threeLetterCode": columns[0]}
            eco_maps.append(eco)

            # per WG direction, hard code this translation.
            eco = {"ecoId": "ECO:0000033",
                   "threeLetterCode": "TAS"}
            eco_maps.append(eco)

            # per WG direction, hard code this translation.
            eco = {"ecoId": "ECO:0005613",
                   "threeLetterCode": "IAGP"}
            eco_maps.append(eco)

            # per WG direction, hard code this translation.
            eco = {"ecoId": "ECO:0007191",
                   "threeLetterCode": "IAGP"}
            eco_maps.append(eco)

            # per WG direction, hard code this translation.
            eco = {"ecoId": "ECO:0005612",
                   "threeLetterCode": "IPM"}
            eco_maps.append(eco)

            # per WG direction, hard code this translation.
            eco = {"ecoId": "ECO:0005611",
                   "threeLetterCode": "IED"}

            eco_maps.append(eco)
            # per WG direction, hard code this translation.
            eco = {"ecoId": "ECO:0007013",
                   "threeLetterCode": "CEA"}

            eco_maps.append(eco)
            # per WG direction, hard code this translation.
            eco = {"ecoId": "ECO:0007014",
                   "threeLetterCode": "CEC"}

            eco_maps.append(eco)

        yield [eco_maps]
