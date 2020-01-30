import logging
import multiprocessing
import uuid

from etl import ETL
from etl.helpers import ETLHelper, AssemblySequenceHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor
from data_manager import DataFileManager
from common import ContextInfo

logger = logging.getLogger(__name__)


class VariationETL(ETL):

    variation_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (a:Allele:Feature {primaryKey: row.alleleId})
                MATCH (g:Gene)-[:IS_ALLELE_OF]-(a)
                
                //Create the variant node and set properties. primaryKey is required.
                MERGE (o:Variant {primaryKey:row.hgvs_nomenclature})
                    ON CREATE SET 
                     o.hgvsNomenclature = row.hgvs_nomenclature,
                     o.genomicReferenceSequence = row.genomicReferenceSequence,
                     o.paddingLeft = row.paddingLeft,
                     o.paddingRight = row.paddingRight,
                     o.genomicVariantSequence = row.genomicVariantSequence,
                     o.dateProduced = row.dateProduced,
                     o.release = row.release,
                     o.dataProviders = row.dataProviders,
                     o.dataProvider = row.dataProvider

                MERGE (o)-[:VARIATION]->(a) 
                MERGE (g)-[:COMPUTED_GENE]->(o) """

    soterms_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:Variant {primaryKey:row.variantId})
            MATCH (s:SOTerm:Ontology {primaryKey:row.soTermId})
            MERGE (o)-[:VARIATION_TYPE]->(s)"""


    genomic_locations_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Variant {primaryKey:row.variantId})
            MATCH (chrm:Chromosome {primaryKey:row.chromosome})

            MERGE (o)-[gchrm:LOCATED_ON]->(chrm)
            MERGE (a:Assembly {primaryKey:row.assembly})
            
            MERGE (gchrmn:GenomicLocation {primaryKey:row.uuid})
              SET gchrmn.start = apoc.number.parseInt(row.start),
                gchrmn.end = apoc.number.parseInt(row.end),
                gchrmn.assembly = row.assembly,
                gchrmn.strand = row.strand,
                gchrmn.chromosome = row.chromosome
                
            MERGE (o)-[of:ASSOCIATION]-(gchrmn)
            MERGE (gchrmn)-[ofc:ASSOCIATION]-(chrm)
    """

    xrefs_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Variant {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_xref_text()

    xrefs_template = """

          USING PERIODIC COMMIT %s
          LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
          

             """

    def __init__(self, config):
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

        logger.info("Loading Variation Data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        logger.info("Finished Loading Variation Data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [VariationETL.variation_query_template, commit_size, "variation_data_" + sub_type.get_data_provider() + ".csv"],
            [VariationETL.genomic_locations_template, commit_size,
             "variant_genomiclocations_" + sub_type.get_data_provider() + ".csv"],
            [VariationETL.soterms_template, commit_size,
             "variant_soterms_" + sub_type.get_data_provider() + ".csv"],
            [VariationETL.xrefs_template, commit_size,
             "variant_xrefs_" + sub_type.get_data_provider() + ".csv"]
        ]

        # Obtain the generator
        generators = self.get_generators(data, sub_type.get_data_provider(), batch_size)
        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_hgvs_nomenclature(self, refseqId, variantType, start_position,
                              end_position, reference_sequence, variant_sequence):
        if start_position is None:
            start_position_str = ""
        else:
            start_position_str = str(start_position)

        if end_position is None:
            end_position_str = ""
        else:
            end_position_str = str(end_position)

        if variant_sequence is None:
            variant_sequence_str = ""
        else:
           variant_sequence_str = variant_sequence

        if reference_sequence is None:
            reference_sequence_str = ""
        else:
           reference_sequence_str = reference_sequence


        hgvs_nomenclature = refseqId.split(":")[1] + ':g.' + start_position_str

        if variantType in ['SO:1000002', 'SO:1000008']:  # point mutation/substitution
            hgvs_nomenclature += reference_sequence_str + ">" + variant_sequence_str
        elif variantType == "SO:0000667": # insertion
            hgvs_nomenclature += '_' + end_position_str + 'ins' + variant_sequence_str
        elif variantType == "SO:0000159": # deletion
            hgvs_nomenclature += '_' + end_position_str + 'del'
        elif variantType == "SO:0002007": # MNV
            hgvs_nomenclature += '_' + end_position_str + 'delins' + variant_sequence_str
        elif variantType == "SO:1000032": # DELIN
            hgvs_nomenclature += '_' + end_position_str + 'delins' + variant_sequence_str
        else:
            hgvs_nomenclature = ''
        return hgvs_nomenclature

    def get_generators(self, variant_data, data_provider, batch_size):
        dataProviders = []
        release = ""
        variants = []
        variant_genomic_locations = []
        variant_so_terms = []
        crossReferences = []
        counter = 0
        dateProduced = variant_data['metaData']['dateProduced']

        dataProviderObject = variant_data['metaData']['dataProvider']

        dataProviderCrossRef = dataProviderObject.get('crossReference')
        dataProvider = dataProviderCrossRef.get('id')
        dataProviderPages = dataProviderCrossRef.get('pages')
        dataProviderCrossRefSet = []

        loadKey = dateProduced + dataProvider + "_VARIATION"

        if dataProviderPages is not None:
            for dataProviderPage in dataProviderPages:
                crossRefCompleteUrl = ETLHelper.get_page_complete_url(dataProvider, self.xrefUrlMap, dataProvider,
                                                                      dataProviderPage)

                dataProviderCrossRefSet.append(ETLHelper.get_xref_dict(dataProvider, dataProvider, dataProviderPage,
                                                                       dataProviderPage, dataProvider,
                                                                       crossRefCompleteUrl,
                                                                       dataProvider + dataProviderPage))

                dataProviders.append(dataProvider)
                logger.debug("data provider: " + dataProvider)

        if 'release' in variant_data['metaData']:
            release = variant_data['metaData']['release']

        assemblies = {}
        for alleleRecord in variant_data['data']:
            chromosome = alleleRecord["chromosome"]
            if chromosome.startswith("chr"):
                chromosome_str = chromosome[3:]
            else:
                chromosome_str = chromosome

            assembly = alleleRecord["assembly"]
            if assembly not in assemblies:
               context_info = ContextInfo()
               data_manager = DataFileManager(context_info.config_file_location)
               assemblies[assembly] = AssemblySequenceHelper(assembly, data_manager)

            SOTermId = alleleRecord.get('type')
            genomicReferenceSequence = alleleRecord.get('genomicReferenceSequence')
            genomicVariantSequence = alleleRecord.get('genomicVariantSequence')

            if genomicReferenceSequence == 'N/A':
                genomicReferenceSequence = ""
            if genomicVariantSequence == 'N/A':
                genomicVariantSequence = ""

            paddingLeft = ""
            paddingRight = ""
            if alleleRecord.get('start') != "" and alleleRecord.get('end') != "":

                # not insertion
                if SOTermId != "SO:0000667" and chromosome_str != "Unmapped_Scaffold_8_D1580_D1567":
                    genomicReferenceSequence = assemblies[assembly].getSequence(chromosome_str,
                                                                                alleleRecord.get('start'),
                                                                                alleleRecord.get('end'))

                if alleleRecord.get('start') < alleleRecord.get('end'):
                    start = alleleRecord.get('start')
                    end = alleleRecord.get('end')
                else:
                    start = alleleRecord.get('end')
                    end = alleleRecord.get('start')

                paddingWidth = 500
                if SOTermId != "SO:0000667": #not insertion
                    start = start - 1
                    end = end + 1

                leftPaddingStart = start - paddingWidth
                if leftPaddingStart < 1:
                    leftPaddingStart = 1

                paddingLeft = assemblies[assembly].getSequence(chromosome_str,
                                                               leftPaddingStart,
                                                               start)
                rightPaddingEnd = end + paddingWidth
                paddingRight = assemblies[assembly].getSequence(chromosome_str,
                                                                end,
                                                                rightPaddingEnd)
            counter = counter + 1
            globalId = alleleRecord.get('alleleId')
            modGlobalCrossRefId = ""
            crossReferences = []

            if self.testObject.using_test_data() is True:
                is_it_test_entry = self.testObject.check_for_test_id_entry(globalId)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            crossRefPrimaryId = alleleRecord.get('sequenceOfReferenceAccessionNumber')
            localCrossRefId = crossRefPrimaryId.split(":")[1]
            prefix = crossRefPrimaryId.split(":")[0]

            crossRefCompleteUrl = ETLHelper.get_no_page_complete_url(localCrossRefId, ETL.xrefUrlMap, prefix,
                                                                     globalId)
            xrefMap = ETLHelper.get_xref_dict(localCrossRefId, prefix, "variant_sequence_of_reference",
                                              "sequence_of_reference_accession_number", globalId, crossRefCompleteUrl,
                                              crossRefPrimaryId + "variant_sequence_of_reference")
            xrefMap['dataId'] = globalId
            if crossRefPrimaryId is not None:
                crossReferences.append(xrefMap)

            if genomicReferenceSequence is not None:
                if len(genomicReferenceSequence) > 1000 and (alleleRecord.get('type') == 'SO:1000002' or alleleRecord.get('type') == 'SO:1000008'):
                    logger.debug(alleleRecord.get('alleleId') + "genomicReferenceSequence")
            if genomicVariantSequence is not None:
                if len(genomicVariantSequence) > 1000 and (alleleRecord.get('type') == 'SO:1000002' or alleleRecord.get('type') == 'SO:1000008'):
                    logger.debug(alleleRecord.get('alleleId') + "genomicVariantSequence")

            hgvs_nomenclature = self.get_hgvs_nomenclature(alleleRecord.get('sequenceOfReferenceAccessionNumber'),
                                                           alleleRecord.get('type'),
                                                           alleleRecord.get('start'),
                                                           alleleRecord.get('end'),
                                                           genomicReferenceSequence,
                                                           genomicVariantSequence)

            if (genomicReferenceSequence is not None and len(genomicReferenceSequence) > 30000)\
                    or (genomicVariantSequence is not None and len(genomicVariantSequence)) > 30000:
                logger.debug(alleleRecord.get('alleleId') + " has too long of a sequence potentionally")

            # TODO: fix typo in MGI Submission for this variant so that it doesn't list a 40K bp point mutation.
            if alleleRecord.get('alleleId') != 'MGI:6113870':

                variant_dataset = {
                "hgvs_nomenclature": hgvs_nomenclature,
                "genomicReferenceSequence": genomicReferenceSequence,
                "genomicVariantSequence": genomicVariantSequence,
                "paddingLeft": paddingLeft,
                "paddingRight": paddingRight,
                "alleleId": alleleRecord.get('alleleId'),
                "dataProviders": dataProviders,
                "dateProduced": dateProduced,
                "loadKey": loadKey,
                "release": release,
                "modGlobalCrossRefId": modGlobalCrossRefId,
                "dataProvider": data_provider
                }

                variant_genomic_location_dataset = {
                    "variantId": hgvs_nomenclature,
                    "assembly": alleleRecord.get('assembly'),
                    "chromosome": chromosome_str,
                    "start": alleleRecord.get('start'),
                    "end": alleleRecord.get('end'),
                    "uuid": str(uuid.uuid4())

                }

                variant_so_term = {
                    "variantId": hgvs_nomenclature,
                    "soTermId": alleleRecord.get('type')
                }

                variant_so_terms.append(variant_so_term)
                variant_genomic_locations.append(variant_genomic_location_dataset)
                variants.append(variant_dataset)

            if counter == batch_size:
                yield [variants, variant_genomic_locations, variant_so_terms, crossReferences]
                variants = []
                variant_genomic_locations =[]
                variant_so_terms =[]
                crossReferences = []

        if counter > 0:
            yield [variants, variant_genomic_locations, variant_so_terms, crossReferences]
