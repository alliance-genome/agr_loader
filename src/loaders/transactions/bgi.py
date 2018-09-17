from .transaction import Transaction
from services import CreateCrossReference
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class BGITransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)

    def bgi_tx(self, gene_dataset, synonyms, secondaryIds, genomicLocations, crossReferences):
        '''
        Loads the BGI data into Neo4j.
        Is name_key necessary with symbol?

        '''

        genomic_locations = """
            UNWIND $data AS row
                    
                MATCH (o:Gene {primaryKey:row.primaryId})
                MERGE (chrm:Chromosome {primaryKey:row.chromosome})

                CREATE (o:Gene)-[gchrm:LOCATED_ON]->(chrm:Chromosome)
                        SET gchrm.start = row.start, 
                        gchrm.end = row.end, 
                        gchrm.assembly = row.assembly, 
                        gchrm.strand = row.strand
                
        """

        gene_secondaryIds = """
        
         UNWIND $data AS row
                MATCH (g:Gene {primaryKey:row.primary_id})
                
                MERGE (second:SecondaryId:Identifier {primaryKey:row.secondary_id})
                    SET second.name = row.secondary_id
                MERGE (g:Gene)-[aka1:ALSO_KNOWN_AS]->(second:SecondaryId:Identifier)
        

        """
        gene_synonyms = """
        
         UNWIND $data AS row
                MATCH (g:Gene {primaryKey:row.primary_id})
                
               MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                    SET syn.name = row.synonym
                MERGE (g:Gene)-[aka2:ALSO_KNOWN_AS]->(syn:Synonym:Identifier)
        """

        gene_query = """

            UNWIND $data AS row

            //Create the load node(s)
            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced,
                l.loadName = "BGI"
                l.release = row.release
                l.dataProviders = row.dataProviders
                l.dataProvider = row.dataProvider

            //Create the Gene node and set properties. primaryKey is required.
            MERGE (o:Gene {primaryKey:row.primaryId})
                SET o.symbol = row.symbol,
                o.taxonId = row.taxonId,
                o.name = row.name,
                o.description = row.description,
                o.geneSynopsisUrl = row.geneSynopsisUrl,
                o.geneSynopsis = row.geneSynopsis,
                o.geneLiteratureUrl = row.geneLiteratureUrl,
                o.geneticEntityExternalUrl = row.geneticEntityExternalUrl,
                o.dateProduced = row.dateProduced,
                o.modGlobalCrossRefId = row.modGlobalCrossRefId,
                o.modCrossRefCompleteUrl = row.modCrossRefCompleteUrl,
                o.modLocalId = row.localId,
                o.modGlobalId = row.modGlobalId,
                o.uuid = row.uuid,
                o.dataProvider = row.dataProvider,
                o.dataProviders = row.dataProviders

            MERGE (l:Load:Entity)-[loadAssociation:LOADED_FROM]-(o:Gene)
            //Create nodes for other identifiers.

            //FOREACH (dataProvider in row.dataProviders |
                //MERGE (dp:DataProvider:Entity {primaryKey:dataProvider})
                  //SET dp.dateProduced = row.dateProduced
                //MERGE (o)-[odp:DATA_PROVIDER]-(dp)
               // MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            MERGE (spec:Species {primaryKey: row.taxonId})
                SET spec.species = row.species,
                    spec.name = row.species
            MERGE (o:Gene)-[:FROM_SPECIES]->(spec:Species)
            MERGE (l:Load:Entity)-[laspec:LOADED_FROM]-(spec:Species)

            //MERGE the SOTerm node and set the primary key.
            MERGE (s:SOTerm:Ontology {primaryKey:row.soTermId})
            MERGE (l:Load:Entity)-[laso:LOADED_FROM]-(s:SOTerm:Ontology)

            //Create the relationship from the gene node to the SOTerm node.
            MERGE (o:Gene)-[x:ANNOTATED_TO]->(s:SOTerm:Ontology) """


        xrefs = """
            UNWIND $data as event
                MATCH (o:Gene {primaryKey:event.dataId})
        
        """ + CreateCrossReference.get_cypher_xref_text("gene")

        if len(gene_dataset) > 0:
            Transaction.execute_transaction(self, gene_query, gene_dataset)
        if len(genomicLocations) > 0:
            Transaction.execute_transaction(self, genomic_locations, genomicLocations)
        if len(secondaryIds) > 0:
            Transaction.execute_transaction(self, gene_secondaryIds, secondaryIds)
        if len(synonyms) > 0:
            Transaction.execute_transaction(self, gene_synonyms, synonyms)
        if len(crossReferences) > 0:
            Transaction.execute_transaction(self, xrefs, crossReferences)

