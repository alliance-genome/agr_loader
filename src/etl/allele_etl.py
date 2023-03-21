"""Allele ETL."""

import logging
import multiprocessing
import uuid

from etl import ETL
from etl.helpers import ETLHelper
from etl.helpers import TextProcessingHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor

logger = logging.getLogger(__name__)


class AlleleETL(ETL):
    """Call AlleleETL."""

    allele_construct_no_gene_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (c:Construct {primaryKey: row.constructId})
                MATCH (s:Species {primaryKey: row.taxonId})
                //Create the Allele node and set properties. primaryKey is required.
                MERGE (o:Allele:Feature {primaryKey:row.primaryId})
                    ON CREATE SET o.symbol = row.symbol,
                    o.taxonId = row.taxonId,
                    o.dateProduced = row.dateProduced,
                    o.release = row.release,
                    o.localId = row.localId,
                    o.globalId = row.globalId,
                    o.uuid = row.uuid,
                    o.symbolText = row.symbolText,
                    o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                    o.dataProviders = row.dataProviders,
                    o.dataProvider = row.dataProvider,
                    o.symbolWithSpecies = row.symbolWithSpecies,
                    o.symbolTextWithSpecies = row.symbolTextWithSpecies,
                    o.description = row.alleleDescription
                MERGE (o)-[:FROM_SPECIES]-(s)
                MERGE (o)-[:CONTAINS]-(c) 
            }
        IN TRANSACTIONS of %s ROWS"""


    allele_construct_gene_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (g:Gene {primaryKey: row.geneId})
                MATCH (c:Construct {primaryKey: row.constructId})
                MATCH (s:Species {primaryKey: row.taxonId})
                //Create the Allele node and set properties. primaryKey is required.
                MERGE (o:Allele:Feature {primaryKey:row.primaryId})
                    ON CREATE SET o.symbol = row.symbol,
                    o.taxonId = row.taxonId,
                    o.dateProduced = row.dateProduced,
                    o.release = row.release,
                    o.localId = row.localId,
                    o.globalId = row.globalId,
                    o.uuid = row.uuid,
                    o.symbolText = row.symbolText,
                    o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                    o.dataProviders = row.dataProviders,
                    o.dataProvider = row.dataProvider,
                    o.symbolWithSpecies = row.symbolWithSpecies,
                    o.symbolTextWithSpecies = row.symbolTextWithSpecies,
                    o.description = row.alleleDescription
                MERGE (o)-[:FROM_SPECIES]-(s)
                MERGE (o)-[:IS_ALLELE_OF]-(g)
                MERGE (o)-[:CONTAINS]-(c)            
            }
        IN TRANSACTIONS of %s ROWS"""
    
    allele_gene_no_construct_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (g:Gene {primaryKey: row.geneId})
                MATCH (s:Species {primaryKey: row.taxonId})
                //Create the Allele node and set properties. primaryKey is required.
                MERGE (o:Allele:Feature {primaryKey:row.primaryId})
                    ON CREATE SET o.symbol = row.symbol,
                    o.taxonId = row.taxonId,
                    o.dateProduced = row.dateProduced,
                    o.release = row.release,
                    o.localId = row.localId,
                    o.globalId = row.globalId,
                    o.uuid = row.uuid,
                    o.symbolText = row.symbolText,
                    o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                    o.dataProviders = row.dataProviders,
                    o.dataProvider = row.dataProvider,
                    o.symbolWithSpecies = row.symbolWithSpecies,
                    o.symbolTextWithSpecies = row.symbolTextWithSpecies,
                    o.description = row.alleleDescription
                MERGE (o)-[:FROM_SPECIES]-(s)
                MERGE (o)-[:IS_ALLELE_OF]->(g)
            }
        IN TRANSACTIONS of %s ROWS"""

    allele_no_gene_no_construct_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (s:Species {primaryKey: row.taxonId})
                //Create the Allele node and set properties. primaryKey is required.
                MERGE (o:Allele:Feature {primaryKey:row.primaryId})
                    ON CREATE SET o.symbol = row.symbol,
                    o.taxonId = row.taxonId,
                    o.dateProduced = row.dateProduced,
                    o.release = row.release,
                    o.localId = row.localId,
                    o.globalId = row.globalId,
                    o.uuid = row.uuid,
                    o.symbolText = row.symbolText,
                    o.modCrossRefCompleteUrl = row.modGlobalCrossRefId,
                    o.dataProviders = row.dataProviders,
                    o.dataProvider = row.dataProvider,
                    o.symbolWithSpecies = row.symbolWithSpecies,
                    o.symbolTextWithSpecies = row.symbolTextWithSpecies,
                    o.description = row.alleleDescription
                MERGE (o)-[:FROM_SPECIES]-(s)
            }
        IN TRANSACTIONS of %s ROWS"""

    allele_secondaryids_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (f:Allele:Feature {primaryKey:row.data_id})
                MERGE (second:SecondaryId {primaryKey:row.secondary_id})
                    SET second.name = row.secondary_id
                MERGE (f)-[aka1:ALSO_KNOWN_AS]->(second)
            }
        IN TRANSACTIONS of %s ROWS"""
    
    allele_synonyms_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row

                MATCH (a:Allele:Feature {primaryKey:row.data_id})
                MERGE(syn:Synonym {primaryKey:row.synonym})
                    SET syn.name = row.synonym
                MERGE (a)-[aka2:ALSO_KNOWN_AS]->(syn)
            }
        IN TRANSACTIONS of %s ROWS"""

    allele_xrefs_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row

                MATCH (o:Allele {primaryKey:row.dataId})
                """ + ETLHelper.get_cypher_xref_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise Object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):

        logger.info("Loading Allele Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        logger.info(filepath)
        data = JSONFile().get_data(filepath)

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return
        ETLHelper.load_release_info(data, sub_type, self.logger)

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        query_list = [
            [AlleleETL.allele_gene_no_construct_query_template, "allele_gene_no_construct_data_" + sub_type.get_data_provider() + ".csv", commit_size],
            [AlleleETL.allele_construct_gene_query_template, "allele_construct_gene_data_" + sub_type.get_data_provider() + ".csv", commit_size],
            [AlleleETL.allele_construct_no_gene_query_template, "allele_construct_no_gene_data_" + sub_type.get_data_provider() + ".csv", commit_size],
            [AlleleETL.allele_no_gene_no_construct_query_template, "allele_no_gene_no_construct_data_" + sub_type.get_data_provider() + ".csv", commit_size],
            [AlleleETL.allele_secondaryids_template, "allele_secondaryids_" + sub_type.get_data_provider() + ".csv", commit_size],
            [AlleleETL.allele_synonyms_template, "allele_synonyms_" + sub_type.get_data_provider() + ".csv", commit_size],
            [AlleleETL.allele_xrefs_template, "allele_xrefs_" + sub_type.get_data_provider() + ".csv", commit_size],
        ]

        # Obtain the generator
        generators = self.get_generators(data, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("Allele-{}: ".format(sub_type.get_data_provider()))
        logger.info("Finished Loading Allele Data: %s" % sub_type.get_data_provider())

    def secondary_process(self, secondarys, data_record):
        """Get secondary ids.

        secondarys: list of dataset items.
        data_record: record to process.
        """
        if data_record.get('secondaryIds') is None:
            return
        for sid in data_record.get('secondaryIds'):
            secondary_id_dataset = {
                "data_id": data_record.get('primaryId'),
                "secondary_id": sid
            }
            secondarys.append(secondary_id_dataset)

    def synonyms_process(self, synonyms, data_record):
        """Get synonyms."""
        if data_record.get('synonyms') is None:
            return
        for syn in data_record.get('synonyms'):
            syn_dataset = {
                "data_id": data_record.get('primaryId'),
                "synonym": syn.strip()
            }
            synonyms.append(syn_dataset)

    def xref_process(self, record, global_id, cross_reference_list):
        """Get xref."""
        valid_pages = ['allele', 'allele/references', 'transgene', 'construct',
                       'transgene/references', 'construct/references']
        if 'crossReferences' not in record:
            return
        for crossRef in record['crossReferences']:
            crossRefId = crossRef.get('id')
            local_crossref_id = crossRefId.split(":")[1]
            prefix = crossRef.get('id').split(":")[0]
            pages = crossRef.get('pages')

            # some pages collection have 0 elements
            if pages is not None and len(pages) > 0:
                for page in pages:
                    if page in valid_pages:
                        mod_global_cross_ref_id = self.etlh.rdh2.return_url_from_key_value(prefix, local_crossref_id, page)
                        xref = ETLHelper.get_xref_dict(local_crossref_id, prefix, page, page, crossRefId,
                                                       mod_global_cross_ref_id, crossRefId + page)
                        xref['dataId'] = global_id
                        cross_reference_list.append(xref)

    def get_generators(self, allele_data, batch_size):
        """Get generators."""
        release = ""
        alleles_no_constrcut_no_gene = []
        alleles_construct_gene = []
        alleles_no_construct = []
        alleles_no_gene = []
        allele_synonyms = []
        allele_secondary_ids = []
        cross_reference_list = []

        counter = 0
        date_produced = allele_data['metaData']['dateProduced']

        # TODO: get SGD to fix their files.
        self.data_providers_process(allele_data)
        loadKey = date_produced + self.data_provider + "_ALLELE"

        if 'release' in allele_data['metaData']:
            release = allele_data['metaData']['release']

        for allele_record in allele_data['data']:
            counter = counter + 1
            global_id = allele_record['primaryId']
            # fixing parsing error on this end while MGI fixes on their end.
            if global_id == 'MGI:3826848':
                description = allele_record.get('description')[:-2]
            else:
                description = allele_record.get('description')

            local_id = global_id.split(":")[1]
            mod_global_cross_ref_id = ""

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            association_type = ''

            short_species_abbreviation = self.etlh.get_short_species_abbreviation(allele_record.get('taxonId'))
            symbol_text = TextProcessingHelper.cleanhtml(allele_record.get('symbol'))

            if allele_record.get('alleleObjectRelations') is not None:
                for relation in allele_record.get('alleleObjectRelations'):
                    gene_id = ''
                    construct_id = ''
                    common = {
                        "alleleDescription": description,
                        "associationType": association_type,
                        "symbol": allele_record.get('symbol'),
                        "globalId": global_id,
                        "localId": local_id,
                        "taxonId": allele_record.get('taxonId'),
                        "dataProvider": self.data_provider,
                        "dataProviders": self.data_providers,
                        "dateProduced": date_produced,
                        "loadKey": loadKey,
                        "release": release,
                        "modGlobalCrossRefId": mod_global_cross_ref_id,
                        "symbolWithSpecies": allele_record.get('symbol') + " (" + short_species_abbreviation + ")",
                        "symbolTextWithSpecies": symbol_text + " (" + short_species_abbreviation + ")",
                        "symbolText": symbol_text,
                        "primaryId": allele_record.get('primaryId'),
                        "uuid": str(uuid.uuid4())
                    }
                    association_type = relation.get('objectRelation').get('associationType')
                    if relation.get('objectRelation').get('gene') is not None:
                        gene_id = relation.get('objectRelation').get('gene')
                    if relation.get('objectRelation').get('construct') is not None:
                        construct_id = relation.get('objectRelation').get('construct')

                    if gene_id != '' and construct_id != '':
                        common["geneId"] = gene_id
                        common["constructId"] = construct_id
                        alleles_construct_gene.append(common)

                    elif construct_id != '' and gene_id == '':
                        common["constructId"] = construct_id
                        common.pop('geneId', None)
                        alleles_no_gene.append(common)

                    elif gene_id != '' and construct_id == '':
                        common["geneId"] = gene_id
                        common.pop('constructId', None)
                        alleles_no_construct.append(common)

                    elif gene_id == '' and construct_id == '':
                        common.pop('geneId', None)
                        common.pop('constructId', None)
                        alleles_no_constrcut_no_gene.append(common)
            else:
                common = {
                    "alleleDescription": description,
                    "associationType": association_type,
                    "symbol": allele_record.get('symbol'),
                    "globalId": global_id,
                    "localId": local_id,
                    "taxonId": allele_record.get('taxonId'),
                    "dataProvider": self.data_provider,
                    "dataProviders": self.data_providers,
                    "dateProduced": date_produced,
                    "loadKey": loadKey,
                    "release": release,
                    "modGlobalCrossRefId": mod_global_cross_ref_id,
                    "symbolWithSpecies": allele_record.get('symbol') + " (" + short_species_abbreviation + ")",
                    "symbolTextWithSpecies": symbol_text + " (" + short_species_abbreviation + ")",
                    "symbolText": symbol_text,
                    "primaryId": allele_record.get('primaryId'),
                    "uuid": str(uuid.uuid4())
                }
                alleles_no_constrcut_no_gene.append(common)

            self.xref_process(allele_record, global_id, cross_reference_list)
            self.synonyms_process(allele_synonyms, allele_record)
            self.secondary_process(allele_secondary_ids, allele_record)

            if counter == batch_size:
                yield [alleles_no_construct, alleles_construct_gene, alleles_no_gene, alleles_no_constrcut_no_gene,
                       allele_secondary_ids, allele_synonyms, cross_reference_list]
                alleles_no_construct = []
                alleles_construct_gene = []
                alleles_no_gene = []
                alleles_no_constrcut_no_gene = []

                allele_secondary_ids = []
                allele_synonyms = []
                cross_reference_list = []
                counter = 0

        if counter > 0:
            yield [alleles_no_construct, alleles_construct_gene, alleles_no_gene, alleles_no_constrcut_no_gene,
                   allele_secondary_ids, allele_synonyms, cross_reference_list]
