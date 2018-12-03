import logging, uuid, gzip, csv
logger = logging.getLogger(__name__)

from transactors import CSVTransactor

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile, S3File

class GOAnnotETL(ETL):

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            
            MATCH (g:Gene {primaryKey:row.gene_id})
            FOREACH (entry in row.annotations |
                MERGE (go:GOTerm:Ontology {primaryKey:entry.go_id})
                MERGE (g)-[:ANNOTATED_TO]->(go)) """

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
            #batch_size = self.data_type_config.get_generator_batch_size()

            generators = self.get_generators(
                file,
                ETLHelper.go_annot_prefix_lookup(sub_type.get_data_provider()),
            )

            query_list = [
                [GOAnnotETL.query_template, commit_size, "go_annot_" + sub_type.get_data_provider() + ".csv"],
            ]
            
            CSVTransactor.execute_transaction(generators, query_list)


    def get_generators(self, file, prefix):
        

        go_annot_dict = {}
        go_annot_list = []

        reader = csv.reader(file, delimiter='\t')
        for line in reader:
            if line[0].startswith('!'):
                continue
            gene_id = prefix + line[1]
            go_id = line[4]

            if gene_id in go_annot_dict:
                go_annot_dict[gene_id]['annotations'].append({"go_id": go_id})
            else:
                go_annot_dict[gene_id] = {
                    'gene_id': gene_id,
                    'annotations': [{"go_id": go_id}],
                }
        # Convert the dictionary into a list of dictionaries for Neo4j.
        # Check for the use of testObject and only return test data if necessary.
        if self.testObject.using_test_data() is True:
            for entry in go_annot_dict:
                if self.testObject.check_for_test_id_entry(go_annot_dict[entry]['gene_id']) is True:
                    go_annot_list.append(go_annot_dict[entry])
                    self.testObject.add_ontology_ids([annotation["go_id"] for annotation in go_annot_dict[entry]['annotations']])
                else:
                    continue
            yield [go_annot_list]
            file.close()
        else:
            for entry in go_annot_dict:
                go_annot_list.append(go_annot_dict[entry])
            yield [go_annot_list]
            file.close()