import logging, os
from test import TestObject

logger = logging.getLogger(__name__)

class ETL(object):

    def __init__(self):

        if "TEST_SET" in os.environ and os.environ['TEST_SET'] == "True":
            logger.warn("WARNING: Test data load enabled.")
            time.sleep(1)
            self.testObject = TestObject(True)
        else:
            self.testObject = TestObject(False)

    def run_etl(self):
        self._load_and_process_data()


    def get_cypher_xref_text():

        return """
                MERGE (id:CrossReference:Identifier {primaryKey:event.primaryKey})
                    SET id.name = event.id,
                     id.globalCrossRefId = event.globalCrossRefId,
                     id.localId = event.localId,
                     id.crossRefCompleteUrl = event.crossRefCompleteUrl,
                     id.prefix = event.prefix,
                     id.crossRefType = event.crossRefType,
                     id.uuid = event.uuid,
                     id.page = event.page,
                     id.primaryKey = event.primaryKey,
                     id.displayName = event.displayName

                MERGE (o)-[gcr:CROSS_REFERENCE]->(id) """
