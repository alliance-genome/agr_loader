from .comment_file import CommentFile
import csv
import codecs
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(name)s:%(lineno)d: %(message)s')
logger = logging.getLogger(__name__)

class CSVFile(object):

    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        logger.info("Loading csv data from %s ..." % (self.filename))
        with codecs.open(self.filename, 'r', 'utf-8') as f:
            reader = csv.reader(CommentFile(f), delimiter='\t')
            rows = []
            for row in reader:
                rows.append(row)
        f.close()    
        return rows
