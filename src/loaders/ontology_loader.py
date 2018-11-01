import time
import logging
from extractors import *
from transactions import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class OntologyLoader(object):

    def __init__(self):
        pass

    def run_loader(self):
        start = time.time()

        ret = self.load_custom()
        self.load_generic_list()

        end = time.time()
        logger.info ("Total time to load ontologies: %s", str(end - start))
        return ret

    def load_custom(self):
        logger.info("Extracting GO data.")
        go_dataset = OExt().get_data("http://snapshot.geneontology.org/ontology/go.obo", "go.obo")
        logger.info("Loading GO data into Neo4j.")
        go_data_loaded = []
        for n in go_dataset.nodes():
            node = go_dataset.node(n)
            if node.get('type') == "PROPERTY":
                continue
            go_data_loaded.append(node)
        GOTransaction().go_tx(go_data_loaded)
        # Does not get cleared because its used later self.go_dataset.clear()

        logger.info("Extracting DO data.")
        do_dataset = OExt().get_data("https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/834f2cacd7876b74915928cafdcaf663ac5f089f/src/ontology/doid.obo", "doid.obo")
        logger.info("Loading DO data into Neo4j.")
        do_data_loaded = []
        for n in do_dataset.nodes():
            node = do_dataset.node(n)
            if node.get('type') == "PROPERTY":
                continue
            if 'oid' in node:   # Primarily filters out the empty nodes
                do_data_loaded.append(node)
        DOTransaction().do_tx(do_data_loaded)
        # Does not get cleared because its used later self.do_dataset.clear()

        logger.info("Downloading MI data.")
        mi_dataset = MIExt().get_data()
        logger.info("Loading MI data into Neo4j.")
        MITransaction().mi_tx(mi_dataset)

        logger.info("Extracting SO data.")
        so_dataset = SOExt().get_data()
        logger.info("Loading SO data into Neo4j.")
        SOTransaction().so_tx(so_dataset)

    def load_generic_list(self):

        gasol = GenericAnatomicalStructureOntologyTransaction()

        list_of_lists = [
            ["http://purl.obolibrary.org/obo/zfa.obo", "zfa.obo", "ZFA"],
            ["http://purl.obolibrary.org/obo/zfs.obo", "zfs.obo", "ZFS"],
            ["http://purl.obolibrary.org/obo/wbbt.obo", "wbbt.obo", "WBBT"],
            ["http://purl.obolibrary.org/obo/cl.obo", "cl.obo", "CL"],
            ["http://purl.obolibrary.org/obo/fbdv.obo", "fbdv-simple.obo", "FBDV"],
            ["http://purl.obolibrary.org/obo/fbbt.obo", "fbbt.obo", "FBBT"],
            ["http://purl.obolibrary.org/obo/fbcv.obo", "fbcv.obo", "FBCV"],
            ["http://purl.obolibrary.org/obo/mmusdv.obo", "mmusdv.obo", "MMUSDV"],
            ["http://purl.obolibrary.org/obo/bspo.obo", "bpso.obo", "BSPO"],
            ["http://purl.obolibrary.org/obo/mmo.obo", "mmo.obo", "MMO"],
            ["http://purl.obolibrary.org/obo/wbls.obo", "wbls.obo", "WBLS"],
            ["http://purl.obolibrary.org/obo/emapa.obo", "emapa.obo", "EMAPA"],
            ["http://www.informatics.jax.org/downloads/reports/adult_mouse_anatomy.obo", "ma.obo", "MA"],
            ["http://ontologies.berkeleybop.org/uberon/basic.obo", "basic.obo", "UBERON"],
        ]

        for generic_list in list_of_lists:
            logger.info("Extracting and loading %s data." % generic_list[2])
            gasol.gaso_tx(ObExto().get_data(generic_list[0], generic_list[1]), generic_list[2] + "Term")
