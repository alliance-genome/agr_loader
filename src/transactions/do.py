from .transaction import Transaction
from services import CreateCrossReference
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DOTransaction(Transaction):

    def __init__(self):
        self.batch_size = 2000

    def do_tx(self, data):
        '''
        Loads the DO data into Neo4j.
        '''

        query = """
            UNWIND $data as row

            //Create the DOTerm node and set properties. primaryKey is required.
            MERGE (doterm:DOTerm:Ontology {primaryKey:row.oid})
                SET doterm.name = row.name,
                 doterm.nameKey = row.name_key,
                 doterm.definition = row.defText,
                 doterm.defLinks = row.defLinksProcessed,
                 doterm.is_obsolete = row.is_obsolete,
                 doterm.subset = row.subset,
                 doterm.doDisplayId = row.oid,
                 doterm.doUrl = row.oUrl,
                 doterm.doPrefix = "DOID",
                 doterm.doId = row.oid,
                 doterm.rgdLink = row.rgd_link,
                 doterm.ratOnlyRgdLink = row.rat_only_rgd_link,
                 doterm.humanOnlyRgdLink = row.human_only_rgd_link,
                 doterm.mgiLink = row.mgi_link,
                 doterm.zfinLink = row.zfin_link,
                 doterm.flybaseLink = row.flybase_link,
                 doterm.wormbaseLink = row.wormbase_link,
                 doterm.sgdLink = row.sgd_link


            FOREACH (entry in row.o_synonyms |
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                MERGE (doterm)-[aka:ALSO_KNOWN_AS]->(syn))

            FOREACH (isa in row.isas |
                MERGE (doterm2:DOTerm:Ontology {primaryKey:isa})
                MERGE (doterm)-[aka:IS_A]->(doterm2))

        """
        queryXref = """
                    
            UNWIND $data as row
             WITH row.crossReferences AS events
                UNWIND events AS event
                    MATCH (o:DOTerm:Ontology {primaryKey:event.oid})

        """ + CreateCrossReference.get_cypher_xref_text("disease_ontology")

        self.execute_transaction_batch(query, data, self.batch_size)
        self.execute_transaction_batch(queryXref, data, self.batch_size)