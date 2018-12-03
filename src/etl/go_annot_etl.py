import logging, uuid, gzip, csv

logger = logging.getLogger(__name__)

from transactors import CSVTransactor

from etl import ETL
from etl.helpers import ETLHelper


class GOAnnotETL(ETL):
    query_template = """
        USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (g:Gene {primaryKey:row.gene_id})
            MATCH (go:GOTerm:Ontology {primaryKey:row.go_id})
            MERGE (g)-[ggo:ANNOTATED_TO]->(go) """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        for sub_type in self.data_type_config.get_sub_type_objects():
            logger.info("Loading GOAnnot Data: %s" % sub_type.get_data_provider())
            filepath = sub_type.get_file_to_download()
            file = gzip.open("tmp/" + filepath, 'rt', encoding='utf-8')

            logger.info("Finished Loading GOAnnot Data: %s" % sub_type.get_data_provider())

            # This order is the same as the lists yielded from the get_generators function.
            # A list of tuples.

            commit_size = self.data_type_config.get_neo4j_commit_size()
            batch_size = self.data_type_config.get_generator_batch_size()

            generators = self.get_generators(
                file,
                ETLHelper.go_annot_prefix_lookup(sub_type.get_data_provider()),
                batch_size
            )

            query_list = [
                [GOAnnotETL.query_template, commit_size, "go_annot_" + sub_type.get_data_provider() + ".csv"],
            ]

            CSVTransactor.execute_transaction(generators, query_list)

    def get_generators(self, file, prefix, batch_size):
        go_annot_list = []
        counter = 0
        reader = csv.reader(file, delimiter='\t')
        for line in reader:
            if line[0].startswith('!'):
                continue
            gene = prefix + line[1]
            logger.info("gene is: " + gene)
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

        if counter > 0:
            yield [go_annot_list]
        file.close()