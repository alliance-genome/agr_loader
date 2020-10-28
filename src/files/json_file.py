"""JSON File"""

import logging
import codecs
import json
import os
import jsonschema as js


class JSONFile(object):
    """JSON File"""

    logger = logging.getLogger(__name__)

    def get_data(self, filename):
        """Get Data"""

        self.logger.debug("Loading JSON data from %s ...", filename)

        if 'PHENOTYPE' in filename:
            self.logger.info(filename)
            self.remove_bom_inplace(filename)
        with codecs.open(filename, 'r', 'utf-8') as file_handle:
            self.logger.debug("Opening JSON file: %s", filename)
            data = json.load(file_handle)
            self.logger.debug("JSON data extracted %s", filename)

        return data

    @staticmethod
    def remove_bom_inplace(path):
        """Removes BOM mark, if it exists, from a file and rewrites it in-place"""

        buffer_size = 4096
        bom_length = len(codecs.BOM_UTF8)

        with codecs.open(path, "r+b") as file_handle:
            chunk = file_handle.read(buffer_size)
            if chunk.startswith(codecs.BOM_UTF8):
                i = 0
                chunk = chunk[bom_length:]
                while chunk:
                    file_handle.seek(i)
                    file_handle.write(chunk)
                    i += len(chunk)
                    file_handle.seek(bom_length, os.SEEK_CUR)
                    chunk = file_handle.read(buffer_size)
                file_handle.seek(-bom_length, os.SEEK_CUR)
                file_handle.truncate()
