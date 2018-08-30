import codecs
import logging

logger = logging.getLogger(__name__)

class TXTFile(object):

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        logger.info("Loading txt data from %s..." % (self.filename))
        lines = []
        with codecs.open(self.filename, 'r', 'utf-8') as f:
            for line in f:
                lines.append(line)
        f.close()    
        return lines
