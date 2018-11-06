from etl import ETL
from .transaction import Transaction
import logging

logger = logging.getLogger(__name__)


class GeoXrefTransaction(Transaction):

    def geo_xref_tx(self, data):

        #TODO: make one query for all xref stanzas instead of duplicating in 4 different files: go.py, do.py, bgi.py, allele.py
        geoXrefQuery = """

                    UNWIND $data AS event
                    MATCH (o:Gene) where o.primaryKey = event.genePrimaryKey

        """ + ETL.get_cypher_xref_text("geo_xref")

        self.execute_transaction(geoXrefQuery, data)
