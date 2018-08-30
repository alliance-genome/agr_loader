from .transaction import Transaction
from services import CreateCrossReference
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class BGITransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def bgi_tx(self, data):
        '''
        Loads the BGI data into Neo4j.
        Is name_key necessary with symbol?

        '''
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(data)
        # quit()

        query = """

            UNWIND $data AS row

            //Create the load node(s)
            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced
                SET l.loadName = "BGI"
                SET l.release = row.release
                SET l.dataProviders = row.dataProviders
                SET l.dataProvider = row.dataProvider

            //Create the Gene node and set properties. primaryKey is required.
            MERGE (o:Gene {primaryKey:row.primaryId})
                SET o.symbol = row.symbol
                SET o.taxonId = row.taxonId
                SET o.name = row.name
                SET o.description = row.description
                SET o.geneSynopsisUrl = row.geneSynopsisUrl
                SET o.geneSynopsis = row.geneSynopsis
                SET o.geneLiteratureUrl = row.geneLiteratureUrl
                SET o.geneticEntityExternalUrl = row.geneticEntityExternalUrl
                SET o.dateProduced = row.dateProduced
                SET o.modGlobalCrossRefId = row.modGlobalCrossRefId
                SET o.modCrossRefCompleteUrl = row.modCrossRefCompleteUrl
                SET o.modLocalId = row.localId
                SET o.modGlobalId = row.modGlobalId
                SET o.uuid = row.uuid
                SET o.dataProvider = row.dataProvider
                SET o.dataProviders = row.dataProviders

            MERGE (l)-[loadAssociation:LOADED_FROM]-(o)
            //Create nodes for other identifiers.

            //FOREACH (dataProvider in row.dataProviders |
                //MERGE (dp:DataProvider:Entity {primaryKey:dataProvider})
                  //SET dp.dateProduced = row.dateProduced
                //MERGE (o)-[odp:DATA_PROVIDER]-(dp)
               // MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            FOREACH (entry in row.secondaryIds |           
                MERGE (second:SecondaryId:Identifier {primaryKey:entry})
                    SET second.name = entry
                MERGE (o)-[aka1:ALSO_KNOWN_AS]->(second)
                MERGE (l)-[las:LOADED_FROM]-(second))

            FOREACH (entry in row.synonyms |           
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                    SET syn.name = entry
                MERGE (o)-[aka2:ALSO_KNOWN_AS]->(syn)
                MERGE (l)-[lasyn:LOADED_FROM]-(syn))

            MERGE (spec:Species {primaryKey: row.taxonId})
                SET spec.species = row.species
                SET spec.name = row.species
            MERGE (o)-[:FROM_SPECIES]->(spec)
            MERGE (l)-[laspec:LOADED_FROM]-(spec)

            //MERGE the SOTerm node and set the primary key.
            MERGE (s:SOTerm:Ontology {primaryKey:row.soTermId})
            MERGE (l)-[laso:LOADED_FROM]-(s)

            //Create the relationship from the gene node to the SOTerm node.
            MERGE (o)-[x:ANNOTATED_TO]->(s)

            //Create the entity relationship to the gene node.
            //MERGE (o)-[c1:CREATED_BY]->(dp)

            WITH o, row.crossReferences AS events
            UNWIND events AS event
        """ + CreateCrossReference.get_cypher_xref_text("gene")

        #TODO: make one query for all xref stanzas instead of duplicating in 4 different files: go.py, do.py, bgi.py, allele.py, geo_xref.py

        locationQuery = """
            UNWIND $data AS row
                WITH row.genomeLocations AS locations
                UNWIND locations AS location
                    //TODO: this is super annoying -- without this second pass of merging gene, it creates new gene nodes!
                    MATCH (o:Gene {primaryKey:location.geneLocPrimaryId})

                    MERGE (chrm:Chromosome {primaryKey:location.chromosome})

                    //gene->chromosome
                    //each location should be unique - if this is merge, then we mistakenly overwrite the relationship properties on each iteration
                    CREATE (o)-[gchrm:LOCATED_ON]->(chrm)
                        SET gchrm.start = location.start 
                        SET gchrm.end = location.end 
                        SET gchrm.assembly = location.assembly 
                        SET gchrm.strand = location.strand
                    
        """
        Transaction.execute_transaction(self, query, data)
        Transaction.execute_transaction(self, locationQuery, data)
