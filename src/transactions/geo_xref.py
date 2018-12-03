import logging

from etl import ETL
from etl.helpers import ETLHelper
from .transaction import Transaction

logger = logging.getLogger(__name__)


class GeoXrefTransaction(Transaction):

    def geo_xref_tx(self, data):

        #TODO: make one query for all xref stanzas instead of duplicating in 4 different files: go.py, do.py, bgi.py, allele.py
        geoXrefQuery = """

                    UNWIND $data AS event
                    MATCH (o:Gene) where o.primaryKey = event.genePrimaryKey

        """ + ETLHelper.get_cypher_xref_text()

        self.execute_transaction(geoXrefQuery, data)
