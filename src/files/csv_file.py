'''CSV file'''

import logging
import codecs
import csv

from .comment_file import CommentFile

class CSVFile(object):
    '''CSV file download'''

    logger = logging.getLogger(__name__)

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        '''Get Data'''

        self.logger.debug("Loading csv data from %s ...", (self.filename))

        with codecs.open(self.filename, 'r', 'utf-8') as file_handle:
            reader = csv.reader(CommentFile(file_handle), delimiter='\t')
            rows = []
            for row in reader:
                rows.append(row)

        return rows
