import os
import gc
import time
import logging

from transactions import *
from mods import *
from extractors import *
from services.gene_descriptions.descriptions_writer import *
from services.gene_descriptions.descriptions_generator import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ModLoader(object):
    def __init__(self):
        # Set size of BGI, disease batches extracted from MOD JSON file
        # for creating Python data structure.
        batch_size = 2000
        self.mods = [MGI(batch_size), Human(batch_size), RGD(batch_size), SGD(batch_size), WormBase(batch_size), ZFIN(batch_size), FlyBase(batch_size)]
        #self.mods = [ZFIN()]


    def run_other_loaders(self):
       
        for mod in self.mods:
            
            if mod.species != 'Homo sapiens':
            
                logger.info("Loading MOD wt expression annotations for %s into Neo4j." % mod.species)
                xpats = mod.load_wt_expression_objects()
                for batch in xpats:
                    WTExpressionTransaction().wt_expression_object_tx(
                        list(batch[0]), list(batch[1]), list(batch[2]), list(batch[3]),
                        list(batch[4]), list(batch[5]), list(batch[6]), list(batch[7]),
                        list(batch[8]), list(batch[9]), list(batch[10]), list(batch[11]),
                        list(batch[12]), mod.species)

            logger.info("Extracting GO annotations for %s." % mod.species)
            go_annots = mod.extract_go_annots()
            logger.info("Loading GO annotations for %s into Neo4j." % mod.species)
            GOAnnotTransaction().go_annot_tx(go_annots)

            logger.info("Extracting GEO annotations for %s." % mod.species)
            geo_xrefs = mod.extract_geo_entrez_ids_from_geo()
            logger.info("Loading GEO annotations for %s." % mod.species)
            GeoXrefTransaction().geo_xref_tx(geo_xrefs)

    def run_other_other_loaders(self):

        this_dir = os.path.split(__file__)[0]
        #initialize gene description generator from config file
        genedesc_generator = GeneDescGenerator(config_file_path=os.path.join(this_dir, "../services", "gene_descriptions", "genedesc_config.yml"),
                                               go_ontology=go_dataset, do_ontology=do_dataset)
        cached_data_fetcher = None

        logger.info("Generating gene descriptions for %s." % mod.species)
        if mod.dataProvider and go_dataset != None and do_dataset != None:
            cached_data_fetcher = genedesc_generator.generate_descriptions(
                go_annotations=go_annots,
                do_annotations=do_annotations,
                do_annotations_allele=do_annotations_allele,
                ortho_data=ortholog_data,
                data_provider=mod.dataProvider,
                cached_data_fetcher=cached_data_fetcher,
                human=isinstance(mod, Human))
            
