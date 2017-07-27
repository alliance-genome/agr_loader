from neo4j.v1 import GraphDatabase
from .transaction import Transaction

class BGITransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def bgi_tx(self, data):
        '''
        Loads the BGI data into Neo4j.
        TODO: is "category" necessary with node type of "gene"?
        Is name_key necessary with symbol?

        '''
        query = """
            UNWIND $data as row 

            //Create the Gene node and set properties.
            CREATE (g:Gene {primary_key:row.primaryId}) 
            SET g.symbol = row.symbol 
            SET g.taxonId = row.taxonId 
            SET g.name = row.name 
            SET g.description = row.description 
            SET g.geneSynopsisUrl = row.geneSynopsisUrl
            SET g.species = row.species
            SET g.geneLiteratureUrl = row.geneLiteratureUrl
            SET g.category = "gene"

            //Create nodes for other identifiers.
            CREATE (second:secondaryId {secondaryIds:row.secondaryIds})
            CREATE (syn:synonyms {synonyms:row.synonyms})
            CREATE (ext:externalIds {externalIds:row.external_ids})

            //Create relationships for other identifiers.
            CREATE (g)-[aka1:ALSO_KNOWN_AS]->(second)
            CREATE (g)-[aka2:ALSO_KNOWN_AS]->(syn)
            CREATE (g)-[aka3:ALSO_KNOWN_AS]->(ext)

            //Create Association nodes for other identifiers.
            CREATE (a1:Association {link_from:row.primaryId, link_to:row.secondaryIds})
            CREATE (a2:Association {link_from:row.primaryId, link_to:row.synonyms})
            CREATE (a3:Association {link_from:row.primaryId, link_to:row.external_ids})

            //Create Association links for other identifiers.
            CREATE (g)-[r1:ASSOC]->(a1)
            CREATE (g)-[r2:ASSOC]->(a2)
            CREATE (g)-[r3:ASSOC]->(a3)

            CREATE (a1)-[r4:ASSOC]->(second)
            CREATE (a2)-[r5:ASSOC]->(syn)
            CREATE (a3)-[r6:ASSOC]->(ext)

            //Create the soTermId node and set the primary key.
            CREATE (s:soTermId {primary_key:row.soTermId})

            //Create the Association node to be used for the gene / soTermId
            CREATE (a4:Association {link_from:row.primaryId, link_to:row.soTermId})

            //Create the relationship from the gene node to association node.
            //Create the relationship from the association node to the soTermId node.
            CREATE (g)-[r7:ASSOC]->(a4)
            CREATE (a4)-[r8:ASSOC]->(s)

            //Create the relationship from the gene node to the soTermId node.
            CREATE (g)-[x:ANNOT_TO]->(s)

            //Create the entity node.
            CREATE (ent:Entity {primary_key:row.dataProvider})
            SET ent.dateProduced = row.dateProduced
            SET ent.release = row.release

            //Create the entity relationship to the appropriate association nodes.
            CREATE (a1)-[c1:CREATED_BY]->(ent)
            CREATE (a2)-[c2:CREATED_BY]->(ent)
            CREATE (a3)-[c3:CREATED_BY]->(ent)
            CREATE (a4)-[c4:CREATED_BY]->(ent)

        """
        Transaction.execute_transaction(self, query, data)

        # "href": None,
        # "gene_biological_process": [],
        # "gene_molecular_function": [],
        # "gene_cellular_component": [],

        # SET g.genomeLocations = row.genomeLocations
        # SET g.crossReferences = row.crossReferences 
        # SET g.modCrossReference = row.modCrossReference
        # Both crossReferences and genomeLocations break the loader:
        # neo4j.exceptions.CypherTypeError: Property values can only be of primitive types or arrays thereof