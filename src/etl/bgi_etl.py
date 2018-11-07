import logging
import uuid

from etl import ETL
from services import UrlService
from transactors import CSVTransactor


logger = logging.getLogger(__name__)

class BGIETL(ETL):

    genomic_locations_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
                
            MATCH (o:Gene {primaryKey:row.primaryId})
            MERGE (chrm:Chromosome {primaryKey:row.chromosome})

            CREATE (o)-[gchrm:LOCATED_ON]->(chrm)
            SET gchrm.start = row.start ,
                gchrm.end = row.end ,
                gchrm.assembly = row.assembly ,
                gchrm.strand = row.strand """
    
    gene_secondaryIds_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey:row.primary_id})
            
            MERGE (second:SecondaryId:Identifier {primaryKey:row.secondary_id})
                SET second.name = row.secondary_id
            MERGE (g)-[aka1:ALSO_KNOWN_AS]->(second) """

    gene_synonyms_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (g:Gene {primaryKey:row.primary_id})
            MATCH (g:Gene {primaryKey:row.primary_id})
            
           MERGE(syn:Synonym:Identifier {primaryKey:row.synonym})
                SET syn.name = row.synonym
            MERGE (g)-[aka2:ALSO_KNOWN_AS]->(syn) """

    gene_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            //Create the load node(s)
            MERGE (l:Load:Entity {primaryKey:row.loadKey})
                SET l.dateProduced = row.dateProduced,
                 l.loadName = "BGI",
                 l.release = row.release,
                 l.dataProviders = row.dataProviders,
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

            MERGE (l)-[loadAssociation:LOADED_FROM]-(o)
            //Create nodes for other identifiers.

            //FOREACH (dataProvider in row.dataProviders |
                //MERGE (dp:DataProvider:Entity {primaryKey:dataProvider})
                  //SET dp.dateProduced = row.dateProduced
                //MERGE (o)-[odp:DATA_PROVIDER]-(dp)
               // MERGE (l)-[ldp:DATA_PROVIDER]-(dp))

            MERGE (spec:Species {primaryKey: row.taxonId})
                SET spec.species = row.species
                SET spec.name = row.species
            MERGE (o)-[:FROM_SPECIES]->(spec)
            MERGE (l)-[laspec:LOADED_FROM]-(spec)

            //MERGE the SOTerm node and set the primary key.
            MERGE (s:SOTerm:Ontology {primaryKey:row.soTermId})
            MERGE (l)-[laso:LOADED_FROM]-(s)

            //Create the relationship from the gene node to the SOTerm node.
            MERGE (o)-[x:ANNOTATED_TO]->(s) """

    xrefs_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Gene {primaryKey:row.dataId}) """ + ETL.get_cypher_xref_text()

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config


    def _load_and_process_data(self):
        
        for mod_config in self.data_type_config.get_mod_configs():
            logger.info("Loading BGI Data: %s" % mod_config.data_provider)
            data = mod_config.get_bgi_data()
            logger.info("Finished Loading BGI Data: %s" % mod_config.data_provider)

            # This order is the same as the lists yielded from the get_generators function.    
            # A list of tuples.

            commit_size = self.data_type_config.get_neo4j_commit_size()
            batch_size = self.data_type_config.get_generator_batch_size()

            # This needs to be in this format (template, param1, params2) others will be ignored
            query_list = [
                [BGIETL.gene_query_template, commit_size, "gene_data_" + mod_config.data_provider + ".csv"],
                [BGIETL.gene_synonyms_template, commit_size, "gene_synonyms_" + mod_config.data_provider + ".csv"],
                [BGIETL.gene_secondaryIds_template, commit_size, "gene_secondarids_" + mod_config.data_provider + ".csv"],
                [BGIETL.genomic_locations_template, commit_size, "gene_genomicLocations_" + mod_config.data_provider + ".csv"],
                [BGIETL.xrefs_template, commit_size, "gene_crossReferences_" + mod_config.data_provider + ".csv"]
            ]

            # Obtain the generator
            generators = self.get_generators(data, mod_config.data_provider, batch_size)

            # Prepare the transaction
            CSVTransactor.execute_transaction(generators, query_list)

    # def save_file(self, data_generator, filename):

    def get_generators(self, gene_data, data_provider, batch_size):

        dateProduced = gene_data['metaData']['dateProduced']
        dataProviders = []
        synonyms = []
        secondaryIds = []
        crossReferences = []
        genomicLocations = []
        gene_dataset = []
        release = None
        counter = 0

        dataProviderObject = gene_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        loadKey = dateProduced + dataProvider + "_BGI"

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = UrlService.get_page_complete_url(dataProvider, ETL.xrefUrlMap, dataProvider, dataProviderPage)
                dataProviderCrossRefSet.append(ETL.get_xref_dict(dataProvider, dataProvider, dataProviderPage, dataProviderPage, dataProvider, crossRefCompleteUrl, dataProvider + dataProviderPage))
                dataProviders.append(dataProvider)
                logger.info("BGI using data provider: " + dataProvider)

        if 'release' in gene_data['metaData']:
            release = gene_data['metaData']['release']

        for geneRecord in gene_data['data']:
            counter = counter + 1


            primary_id = geneRecord['primaryId']
            global_id = geneRecord['primaryId']

            local_id = global_id.split(":")[1]
            geneLiteratureUrl = ""
            geneticEntityExternalUrl = ""
            modCrossReferenceCompleteUrl = ""
            taxonId = geneRecord.get("taxonId")

            if geneRecord['taxonId'] == "NCBITaxon:9606" or geneRecord['taxonId'] == "NCBITaxon:10090":
                local_id = geneRecord['primaryId']

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(primary_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            #TODO: can we split this off into another class?

            if 'crossReferences' in geneRecord:
                for crossRef in geneRecord['crossReferences']:
                    if ':' in crossRef.get('id'):
                        crossRefId = crossRef.get('id')
                        localCrossRefId = crossRefId.split(":")[1]
                        prefix = crossRef.get('id').split(":")[0]
                        pages = crossRef.get('pages')
                        globalXrefId = crossRef.get('id')
                        displayName = globalXrefId

                        # some pages collection have 0 elements
                        if pages is not None and len(pages) > 0:
                            for page in pages:
                                modCrossReferenceCompleteUrl = ""
                                geneLiteratureUrl = ""
                                displayName = ""

                                crossRefCompleteUrl = UrlService.get_page_complete_url(localCrossRefId, ETL.xrefUrlMap, prefix, page)

                                if page == 'gene':
                                    modCrossReferenceCompleteUrl = UrlService.get_page_complete_url(localCrossRefId, ETL.xrefUrlMap, prefix, prefix + page)

                                geneticEntityExternalUrl = UrlService.get_page_complete_url(localCrossRefId, ETL.xrefUrlMap, prefix, prefix + page)

                                if page == 'gene/references':
                                    geneLiteratureUrl = UrlService.get_page_complete_url(localCrossRefId, ETL.xrefUrlMap, prefix, prefix + page)

                                if page == 'gene/spell':
                                    displayName='Serial Patterns of Expression Levels Locator (SPELL)'

                                # TODO: fix generic_cross_reference in SGD, RGD

                                if page == 'generic_cross_reference':
                                    crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId, ETL.xrefUrlMap, prefix, primary_id)

                                # TODO: fix gene/disease xrefs for SGD once resourceDescriptor change in develop
                                # makes its way to the release branch.

                                if page == 'gene/disease' and taxonId == 'NCBITaxon:559292':
                                    crossRefCompleteUrl = "https://www.yeastgenome.org/locus/"+local_id+"/disease"

                                xrefMap = ETL.get_xref_dict(localCrossRefId, prefix, page, page, displayName, crossRefCompleteUrl, globalXrefId+page)
                                xrefMap['dataId'] = primary_id
                                crossReferences.append(xrefMap)

                        else:
                            if prefix == 'PANTHER':  # TODO handle in the resourceDescriptor.yaml
                                crossRefPrimaryId = crossRef.get('id') + '_' + primary_id
                                crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId, ETL.xrefUrlMap, prefix, primary_id)
                                xrefMap = ETL.get_xref_dict(localCrossRefId, prefix, "gene/panther", "gene/panther", displayName, crossRefCompleteUrl, crossRefPrimaryId + "gene/panther")
                                xrefMap['dataId'] = primary_id
                                crossReferences.append(xrefMap)

                            else:
                                crossRefPrimaryId = crossRef.get('id')
                                crossRefCompleteUrl = UrlService.get_no_page_complete_url(localCrossRefId, ETL.xrefUrlMap, prefix, primary_id)
                                xrefMap = ETL.get_xref_dict(localCrossRefId, prefix, "generic_cross_reference", "generic_cross_reference", displayName, crossRefCompleteUrl, crossRefPrimaryId + "generic_cross_reference")
                                xrefMap['dataId'] = primary_id
                                crossReferences.append(xrefMap)

            gene = {
                "symbol": geneRecord.get('symbol'),
                "name": geneRecord.get('name'),
                "geneticEntityExternalUrl": geneticEntityExternalUrl,
                "description": geneRecord.get('description'),
                "soTermId": geneRecord['soTermId'],
                "geneSynopsis": geneRecord.get('geneSynopsis'),
                "geneSynopsisUrl": geneRecord.get('geneSynopsisUrl'),
                "taxonId": geneRecord['taxonId'],
                "species": ETL.species_lookup_by_taxonid(taxonId),
                "geneLiteratureUrl": geneLiteratureUrl,
                "name_key": geneRecord.get('symbol'),
                "primaryId": primary_id,
                "category": "gene",
                "dateProduced": dateProduced,
                "dataProviders": dataProviders,
                "dataProvider": data_provider,
                "release": release,
                "href": None,
                "uuid": str(uuid.uuid4()),
                "modCrossRefCompleteUrl": modCrossReferenceCompleteUrl,
                "localId": local_id,
                "modGlobalCrossRefId": global_id,
                "modGlobalId": global_id,
                "loadKey": loadKey
            }
            gene_dataset.append(gene)

            if 'genomeLocations' in geneRecord:
                for genomeLocation in geneRecord['genomeLocations']:
                    chromosome = genomeLocation['chromosome']
                    assembly = genomeLocation['assembly']
                    if 'startPosition' in genomeLocation:
                        start = genomeLocation['startPosition']
                    else:
                        start = None
                    if 'endPosition' in genomeLocation:
                        end = genomeLocation['endPosition']
                    else:
                        end = None
                    if 'strand' in geneRecord['genomeLocations']:
                        strand = genomeLocation['strand']
                    else:
                        strand = None
                    genomicLocations.append(
                        {"primaryId": primary_id, "chromosome": chromosome, "start":
                            start, "end": end, "strand": strand, "assembly": assembly})

            if geneRecord.get('synonyms') is not None:
                for synonym in geneRecord.get('synonyms'):
                    geneSynonym = {
                        "primary_id": primary_id,
                        "synonym": synonym
                    }
                    synonyms.append(geneSynonym)

            if geneRecord.get('secondaryIds') is not None:
                for secondaryId in geneRecord.get('secondaryIds'):
                    geneSecondaryId = {
                        "primary_id": primary_id,
                        "secondary_id": secondaryId
                    }
                    secondaryIds.append(geneSecondaryId)
            
            # Establishes the number of genes to yield (return) at a time.
            if counter == batch_size:
                counter = 0
                yield [gene_dataset, synonyms, secondaryIds, genomicLocations, crossReferences]
                gene_dataset = []
                synonyms = []
                secondaryIds = []
                genomicLocations = []
                crossReferences = []

        if counter > 0:
            yield [gene_dataset, synonyms, secondaryIds, genomicLocations, crossReferences]
