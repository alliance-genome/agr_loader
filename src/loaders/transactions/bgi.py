from neo4j.v1 import GraphDatabase
from .transaction import Transaction

class BGITransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def bgi_tx(self, data):
        '''
        Loads the BGI data into Neo4j.
        Is name_key necessary with symbol?

        '''
        query = """
            UNWIND $data as row 

            //Create the Gene node and set properties. primaryKey is required.
            CREATE (g:Gene {primaryKey:row.primaryId, dateProduced:row.dateProduced, dataProvider:row.dataProvider})
            SET g.symbol = row.symbol 
            SET g.taxonId = row.taxonId 
            SET g.name = row.name 
            SET g.description = row.description 
            SET g.geneSynopsisUrl = row.geneSynopsisUrl
            SET g.species = row.species
            SET g.geneLiteratureUrl = row.geneLiteratureUrl
            SET g.gene_biological_process = row.gene_biological_process
            SET g.gene_molecular_function = row.gene_molecular_function
            SET g.gene_cellular_component = row.gene_cellular_component

            //Create nodes for other identifiers.
            CREATE (second:SecondaryIds {secondaryIds:row.secondaryIds})
            CREATE (syn:Synonyms {synonyms:row.synonyms})
            CREATE (ext:ExternalIds {externalIds:row.external_ids})

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

            //Create the SOTerm node and set the primary key.
            CREATE (s:SOTerm {primaryKey:row.soTermId})

            //Create the Association node to be used for the gene / SOTerm
            CREATE (a4:Association {link_from:row.primaryId, link_to:row.soTermId})

            //Create the relationship from the gene node to association node.
            //Create the relationship from the association node to the SOTerm node.
            CREATE (g)-[r7:ASSOC]->(a4)
            CREATE (a4)-[r8:ASSOC]->(s)

            //Create the relationship from the gene node to the SOTerm node.
            CREATE (g)-[x:ANNOT_TO]->(s)

            //Create the entity node.
            CREATE (ent:Entity {primaryKey:row.dataProvider})
            SET ent.dateProduced = row.dateProduced
            SET ent.release = row.release

            //Create the entity relationship to the appropriate association nodes.
            CREATE (a1)-[c1:CREATED_BY]->(ent)
            CREATE (a2)-[c2:CREATED_BY]->(ent)
            CREATE (a3)-[c3:CREATED_BY]->(ent)
            CREATE (a4)-[c4:CREATED_BY]->(ent)

        """
        Transaction.execute_transaction(self, query, data)

        # The properties below need to be assigned / resolved:
        # "href": None,

        # Both crossReferences and genomeLocations break the loader:
        # neo4j.exceptions.CypherTypeError: Property values can only be of primitive types or arrays thereof
        # SET g.genomeLocations = row.genomeLocations
        # SET g.crossReferences = row.crossReferences 
        # SET g.modCrossReference = row.modCrossReference