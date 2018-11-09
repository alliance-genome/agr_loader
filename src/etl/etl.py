import logging, os
from test import TestObject
import time

from etl.helpers import OBOHelper
from etl.helpers import ResourceDescriptorHelper


logger = logging.getLogger(__name__)

class ETL(object):

    xrefUrlMap = ResourceDescriptorHelper().get_data()
    oboHelper = OBOHelper()

    def __init__(self):

        if "TEST_SET" in os.environ and os.environ['TEST_SET'] == "True":
            logger.warn("WARNING: Test data load enabled.")
            time.sleep(1)
            self.testObject = TestObject(True)
        else:
            self.testObject = TestObject(False)

    def run_etl(self):
        self._load_and_process_data()