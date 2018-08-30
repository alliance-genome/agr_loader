import xml.etree.ElementTree as ElementTree
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(name)s:%(lineno)d: %(message)s')
logger = logging.getLogger(__name__)

class XMLFile(object):

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        logger.info("Parsing XML data from %s..." % self.filename)
        tree = ElementTree.parse(self.filename)
        root = tree.getroot()

        return root
