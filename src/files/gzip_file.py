import gzip
import shutil
import logging
import os


class GZIPFile(object):

    def __init__(self, filename):
        self.filename = filename
        self.logger = logging.getLogger(__name__)

    def extract(self):
        """Check whether the file is gzip encoded.
        If so, attempt to extract the data."""

        output_filename = os.path.splitext(self.filename)[0]
        self.logger.info('Decompressing file: %s to %s', self.filename, output_filename)
        with gzip.open(self.filename, 'rb') as f_in:
            with open(output_filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
