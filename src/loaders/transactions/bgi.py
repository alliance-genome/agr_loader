from neo4j.v1 import GraphDatabase
from .transaction import Transaction
import pprint

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
            UNWIND $data as row

            //Create the Gene node and set properties. primaryKey is required.
            CREATE (g:Gene {primaryKey:row.primaryId})
            SET g.symbol = row.symbol
            SET g.taxonId = row.taxonId
            SET g.name = row.name
            SET g.description = row.description
            SET g.geneSynopsisUrl = row.geneSynopsisUrl
            SET g.geneLiteratureUrl = row.geneLiteratureUrl
            SET g.geneticEntityExternalUrl = row.geneticEntityExternalUrl
            SET g.dateProduced = row.dateProduced
            SET g.dataProvider = row.dataProvider

            //Create nodes for other identifiers.

            FOREACH (entry in row.secondaryIds |           
                MERGE (second:SecondaryId:Identifier {primaryKey:entry})
                ON CREATE SET second.name = entry
                MERGE (g)-[aka1:ALSO_KNOWN_AS]->(second))

            FOREACH (entry in row.synonyms |           
                MERGE (syn:Synonym:Identifier {primaryKey:entry})
                ON CREATE SET syn.name = entry
                MERGE (g)-[aka2:ALSO_KNOWN_AS]->(syn))

            FOREACH (entry in row.external_ids |           
                MERGE (ext:ExternalId:Identifier {primaryKey:entry})
                ON CREATE SET ext.name = entry
                MERGE (g)-[aka3:ALSO_KNOWN_AS]->(ext))

            MERGE (spec:Species {primaryId: row.taxonId})
            ON CREATE SET spec.species = row.species
            ON CREATE SET spec.name = row.species
            MERGE (g)-[:FROM_SPECIES]->(spec)

            //MERGE the SOTerm node and set the primary key.
            MERGE (s:SOTerm:Ontology {primaryKey:row.soTermId})

            //Create the relationship from the gene node to the SOTerm node.
            MERGE (g)-[x:ANNOTATED_TO]->(s)

            //Merge the entity node.
            MERGE (ent:Entity {primaryKey:row.dataProvider})
            ON CREATE SET ent.dateProduced = row.dateProduced
            ON CREATE SET ent.release = row.release

            //Create the entity relationship to the gene node.
            MERGE (g)-[c1:CREATED_BY]->(ent)

            WITH row.crossReferences as events
            UNWIND events as event
                MERGE (id:CrossReference:Entity {primaryKey:event.id})
                ON CREATE SET id.name = event.id
                ON CREATE SET id.globalCrosssrefId = event.crossRef
                ON CREATE SET id.localId = event.localId
                ON CREATE SET id.crossrefCompleteUrl = event.crossrefCompleteUrl
                MERGE (g)-[gcr:CROSS_REFERENCE]->(id)
        """

        locationQuery = """
            UNWIND $data as row
                MERGE (g:Gene {primaryKey:row.primaryId})
                WITH row.genomeLocations as locations
                UNWIND locations as location
                    MERGE (chrm:Chromosome {primaryKey:location.chromosome})
                    MERGE (g)-[gchrm:LOCATED_ON]->(chrm)
                    //TODO: would be nice to have a key here -- to avoid duplicate nodes, merge doesn't have anything to merge on.
                    CREATE (loc:Location {start:location.start, end:location.end, assembly:location.assembly, strand:location.strand})
                    MERGE (lc:Association {chromosome:location.chromosome})
                    MERGE (lc)-[locc:ANNOTATED_TO]->(loc)
                    MERGE (lc)-[gal:ANNOATED_TO]->(g)
                    MERGE (lc)-[chrmlc:ANNOTATED_TO]->(chrm)

        """
        Transaction.execute_transaction(self, query, data)
        Transaction.execute_transaction(self, locationQuery, data)