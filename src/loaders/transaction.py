from neo4j.v1 import GraphDatabase

class Transaction():

    def __init__(self, graph):
        self.graph = graph
        self.tracking_dict = {}

    def execute_transaction(self, query, data):
        with self.graph.session() as session:
            with session.begin_transaction() as tx:
                tx.run(query, data=data)

    def batch_load_simple(self, label, data, primary_key):
        '''
        Loads a list of dictionaries (data) into nodes with label (label) and primary_key (primary_key).
        Dictionary entries must contain the string (primary_key) as the key of a key : value pair.
        '''
        query = """
            UNWIND $data as row \
            MERGE (n:%s {primary_key:row.%s})
        """ % (label, primary_key)

        self.execute_transaction(query, data)

    def bgi_tx(self, data):
        '''
        Loads the BGI data into Neo4j.
        TODO: is "category" necessary with node type of "gene"?
        Is name_key necessary with symbol?

        '''
        query = """
            UNWIND $data as row 

            //Merge the Gene node and set properties.
            MERGE (g:Gene {primary_key:row.primaryId}) 
            SET g.symbol = row.symbol 
            SET g.taxonId = row.taxonId 
            SET g.name = row.name 
            SET g.description = row.description 
            SET g.synonyms = row.synonyms
            SET g.secondaryIds = row.secondaryIds
            SET g.geneSynopsisUrl = row.geneSynopsisUrl
            SET g.species = row.species
            SET g.externalIds = row.external_ids
            SET g.geneLiteratureUrl = row.geneLiteratureUrl
            SET g.category = "gene"

            //Merge the soTermId node and set the primary key.
            MERGE (s:soTermId {primary_key:row.soTermId})

            //Merge the Association node to be used for the gene / soTermId
            MERGE (a:Association {link_from:row.primaryId, link_to:row.soTermId})

            //Merge the relationship from the gene node to association node.
            //Merge the relationship from the association node to the soTermId node.
            MERGE (g)-[r:ASSOC]-(a)
            MERGE (a)-[z:ASSOC]-(s)

            //Merge the relationship from the gene node to the soTermId node.
            MERGE (g)-[x:ANNOT_TO]-(s)

            //Merge the entity node.
            MERGE (ent:Entity {primary_key:row.dataProvider})
            SET ent.dateProduced = row.dateProduced
            SET ent.release = row.release

            //Merge the entity to the appropriate association nodes.
            MERGE (a)-[c1:CREATED_BY]-(ent)

        """
        self.execute_transaction(query, data)

        # "href": None,
        # "gene_biological_process": [],
        # "gene_molecular_function": [],
        # "gene_cellular_component": [],

        # SET g.genomeLocations = row.genomeLocations
        # SET g.crossReferences = row.crossReferences 
        # SET g.modCrossReference = row.modCrossReference
        # Both crossReferences and genomeLocations break the loader:
        # neo4j.exceptions.CypherTypeError: Property values can only be of primitive types or arrays thereof