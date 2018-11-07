import logging

import xml.etree.ElementTree as ElementTree


logger = logging.getLogger(__name__)

class XMLFile(object):

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        logger.info("Parsing XML data from %s..." % self.filename)
        tree = ElementTree.parse(self.filename)
        root = tree.getroot()

        return root
