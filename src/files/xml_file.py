import xml.etree.ElementTree as ElementTree


class XMLFile(object):

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        print("Parsing XML data from %s..." % self.filename)
        tree = ElementTree.parse(self.filename)
        root = tree.getroot()

        return root
