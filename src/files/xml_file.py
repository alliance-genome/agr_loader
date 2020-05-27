"""XML File"""

import logging



import xml.etree.ElementTree as ElementTree


class XMLFile():
    """XML File"""

    logger = logging.getLogger(__name__)

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        """Get Data"""

        self.logger.debug("Parsing XML data from %s...", self.filename)
        tree = ElementTree.parse(self.filename)
        root = tree.getroot()

        return root
