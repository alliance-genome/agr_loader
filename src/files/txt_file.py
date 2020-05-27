"""Text File"""

import logging
import codecs

class TXTFile():
    """Text File"""

    logger = logging.getLogger(__name__)

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        """Get Data"""

        self.logger.info("Loading txt data from %s...", self.filename)

        lines = []
        with codecs.open(self.filename, 'r', 'utf-8') as file_handle:
            for line in file_handle:
                lines.append(line)

        return lines
