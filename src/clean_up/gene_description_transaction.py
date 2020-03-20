''''Gene Descriptions Transactions'''

import logging
import os


class GeneDescriptionTransaction():
    '''Gene Description Transaction'''

    logger = logging.getLogger(__name__)

    def __init__(self):
        self.batch_size = 3000

    def gd_tx(self, data):
        '''
        Loads gene descriptions data into Neo4j.
        '''
        query = """
            UNWIND $data as 

            MATCH (g:Gene {primaryKey:row.gene_id})
                WHERE g.automatedGeneSynopsis is NULL
                SET g.automatedGeneSynopsis = row.description
        """

        self.execute_transaction_batch(query, data, self.batch_size)

    def run_other_other_loaders(self):

        this_dir = os.path.split(__file__)[0]
        #initialize gene description generator from config file
        genedesc_generator = GeneDescGenerator(config_file_path=\
                                          os.path.join(this_dir,
                                                       "../services",
                                                       "gene_descriptions",
                                                       "genedesc_config.yml"),
                                               go_ontology=go_dataset,
                                               do_ontology=do_dataset)
        cached_data_fetcher = None

        self.logger.info("Generating gene descriptions for %s.", mod.species)
        if mod.dataProvider and go_dataset != None and do_dataset != None:
            cached_data_fetcher = genedesc_generator.generate_descriptions
            go_annotations = go_annots,
            do_annotations = do_annotations,
            do_annotations_allele = do_annotations_allele,
            ortho_data = ortholog_data,
            data_provider = mod.dataProvider,
            cached_data_fetcher = cached_data_fetcher,
            human = isinstance(mod, Human)
