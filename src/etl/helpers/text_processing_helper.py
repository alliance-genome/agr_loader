import logging
import re
logger = logging.getLogger(__name__)


class TextProcessingHelper(object):

    @staticmethod
    def cleanhtml(raw_html):
        cleanr = re.compile('</.*?>')
        cleantext = re.sub(cleanr, '>', raw_html)
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '<', cleantext)
        return cleantext