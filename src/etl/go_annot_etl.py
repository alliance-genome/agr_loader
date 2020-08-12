"""GO Anootation ETL"""

import os
import logging
import csv
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from transactors import CSVTransactor, Neo4jTransactor


class GOAnnotETL(ETL):
    """GO Annotation ETL"""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    main_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
            MATCH (g:Gene {primaryKey:row.gene_id})
            MATCH (go:GOTerm:Ontology {primaryKey:row.go_id})
            CREATE (g)-[:ANNOTATED_TO]->(go) """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        query_tracking_list = multiprocessing.Manager().list()
        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type,
                                              args=(sub_type, query_tracking_list))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

        queries = []
        for item in query_tracking_list:
            queries.append(item)

        Neo4jTransactor.execute_query_batch(queries)
        self.error_messages()

    def _process_sub_type(self, sub_type, query_tracking_list):
        self.logger.info("Loading GOAnnot Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_file_to_download()
        filepath = os.path.join('tmp/', filepath)
        self.logger.info("goannot path: %s", filepath)
        file = open(filepath, "r")

        self.logger.info("Finished Loading GOAnnot Data: %s", sub_type.get_data_provider())

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(
            file,
            ETLHelper.go_annot_prefix_lookup(sub_type.get_data_provider()),
            batch_size)

        query_template_list = [
            [self.main_query_template, commit_size,
             "go_annot_" + sub_type.get_data_provider() + ".csv"],
        ]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)

        for item in query_and_file_list:
            query_tracking_list.append(item)
        self.error_messages("POST_PST: ")

    def get_generators(self, file, prefix, batch_size):
        """Create Generators"""

        go_annot_list = []
        counter = 0
        reader = csv.reader(file, delimiter='\t')
        line_counter = 1
        try:
            for line in reader:
                line_counter += 1
                if line[0].startswith('!'):
                    continue
                if 'HGNC' in line[1]:
                    gene = line[1]
                else:
                    gene = prefix + line[1]

                go_id = line[4]
                go_annot_dict = {
                    'gene_id': gene,
                    'go_id': go_id
                }
                counter = counter + 1
                go_annot_list.append(go_annot_dict)
                if counter == batch_size:
                    counter = 0
                    yield [go_annot_list]
                    go_annot_list = []
        except Exception:
            self.logger.error("GAF file is failing %s at line %s", file.name, str(line_counter))

        if counter > 0:
            yield [go_annot_list]
        file.close()
