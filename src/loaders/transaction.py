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
        '''
        query = """
            UNWIND $data as row 
            MERGE (n:Gene {primary_key:row.primaryId}) 
            SET n.symbol = row.symbol 
            SET n.taxonId = row.taxonId 
            SET n.name = row.name 
            SET n.description = row.description 
            SET n.synonyms = row.synonyms
            SET n.secondaryIds = row.secondaryIds
            SET n.geneSynopsisUrl = row.geneSynopsisUrl
            SET n.species = row.species
            SET n.external_ids = row.external_ids
        """
        self.execute_transaction(query, data)

        # "soTermId": geneRecord['soTermId'],
        # "soTermName": None,
        # "diseases": [],
        # "external_ids": external_ids,
        # "gene_biological_process": [],
        # "gene_molecular_function": [],
        # "gene_cellular_component": [],
        # "genomeLocations": genomic_locations,
        # "geneLiteratureUrl": geneRecord.get('geneLiteratureUrl'),
        # "name_key": geneRecord['symbol'],
        # "primaryId": primary_id,
        # "crossReferences": crossReferences,
        # "modCrossReference": modCrossReference,
        # "category": "gene",
        # "dateProduced": dateProduced,
        # "dataProvider": dataProvider,
        # "release": release,
        # "href": None