"""Protein Sequence ETL"""

import logging
import multiprocessing
import uuid

from etl import ETL
from etl.helpers import ETLHelper, AssemblySequenceHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor
from data_manager import DataFileManager
from loader_common import ContextInfo


class ProteinSequenceETL(ETL):
    """ProteinSequence ETL"""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    fetch_exons_query_template = """

                MATCH (a:Allele {primaryKey: row.alleleId})
                MATCH (g:Gene)-[:IS_ALLELE_OF]-(a)

    """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            process = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            process.start()
            thread_pool.append(process)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):

        self.logger.info("Loading Protein Sequences: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        self.logger.info("Finished Loading Protein Sequences: %s", sub_type.get_data_provider())

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored

        query_template_list = [
            [self.fetch_exons_query_template, commit_size],
        ]

        generators = self.get_generators(data, batch_size)
        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, variant_data, batch_size):
        """Get Generators"""

        variants = []

        assemblies = {}

        for allele_record in variant_data['data']:
            chromosome = allele_record["chromosome"]
            if chromosome.startswith("chr"):
                chromosome_str = chromosome[3:]
            else:
                chromosome_str = chromosome

            assembly = allele_record["assembly"]

            if assembly not in assemblies:
                self.logger.info(assembly)
                context_info = ContextInfo()
                data_manager = DataFileManager(context_info.config_file_location)
                assemblies[assembly] = AssemblySequenceHelper(assembly, data_manager)

            so_term_id = allele_record.get('type')
            genomic_reference_sequence = allele_record.get('genomicReferenceSequence')
            genomic_variant_sequence = allele_record.get('genomicVariantSequence')

            if genomic_reference_sequence == 'N/A':
                genomic_reference_sequence = ""
            if genomic_variant_sequence == 'N/A':
                genomic_variant_sequence = ""

            padding_left = ""
            padding_right = ""
            if allele_record.get('start') != "" and allele_record.get('end') != "":

                # not insertion
                if so_term_id != "SO:0000667" and chromosome_str != "Unmapped_Scaffold_8_D1580_D1567":
                    genomic_reference_sequence = assemblies[assembly].get_sequence(chromosome_str,
                                                                                   allele_record.get('start'),
                                                                                   allele_record.get('end'))

                if allele_record.get('start') < allele_record.get('end'):
                    start = allele_record.get('start')
                    end = allele_record.get('end')
                else:
                    start = allele_record.get('end')
                    end = allele_record.get('start')

                padding_width = 500
                if so_term_id != "SO:0000667":  # not insertion
                    start = start - 1
                    end = end + 1

                left_padding_start = start - padding_width
                if left_padding_start < 1:
                    left_padding_start = 1

                padding_left = assemblies[assembly].get_sequence(chromosome_str,
                                                                 left_padding_start,
                                                                 start)
                right_padding_end = end + padding_width
                padding_right = assemblies[assembly].get_sequence(chromosome_str,
                                                                  end,
                                                                  right_padding_end)
            counter = counter + 1
            global_id = allele_record.get('alleleId')
            mod_global_cross_ref_id = ""
            cross_references = []

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            cross_ref_primary_id = allele_record.get('sequenceOfReferenceAccessionNumber')
            local_cross_ref_id = cross_ref_primary_id.split(":")[1]
            prefix = cross_ref_primary_id.split(":")[0]

            cross_ref_complete_url = ETLHelper.get_no_page_complete_url(local_cross_ref_id,
                                                                        ETL.xref_url_map,
                                                                        prefix,
                                                                        global_id)
            xref_map = ETLHelper.get_xref_dict(local_cross_ref_id,
                                               prefix,
                                               "variant_sequence_of_reference",
                                               "sequence_of_reference_accession_number",
                                               global_id,
                                               cross_ref_complete_url,
                                               cross_ref_primary_id + "variant_sequence_of_reference")

            xref_map['dataId'] = global_id
            if cross_ref_primary_id is not None:
                cross_references.append(xref_map)

            if genomic_reference_sequence is not None:
                if len(genomic_reference_sequence) > 1000 and (allele_record.get('type') == 'SO:1000002'
                                                               or allele_record.get('type') == 'SO:1000008'):
                    self.logger.debug("%s genomicReferenceSequence", allele_record.get('alleleId'))

            if genomic_variant_sequence is not None:
                if len(genomic_variant_sequence) > 1000 and (allele_record.get('type')
                                                             in ['SO:1000002', 'SO:1000008']):
                    self.logger.debug("%s genomicVariantSequence", allele_record.get('alleleId'))

            hgvs_nomenclature, hgvs_synonym = self.get_hgvs_nomenclature(
                allele_record.get('sequenceOfReferenceAccessionNumber'),
                allele_record.get('type'),
                allele_record.get('start'),
                allele_record.get('end'),
                genomic_reference_sequence,
                genomic_variant_sequence,
                allele_record.get('assembly'),
                chromosome_str)

            if (genomic_reference_sequence is not None and len(genomic_reference_sequence) > 30000) \
                    or (genomic_variant_sequence is not None and len(genomic_variant_sequence)) > 30000:
                self.logger.debug("%s has too long of a sequence potentionally",
                                  allele_record.get('alleleId'))

            # TODO: fix typo in MGI Submission for this variant so
            # that it doesn't list a 40K bp point mutation.
            if allele_record.get('alleleId') != 'MGI:6113870':
                variant_dataset = {
                    "hgvs_nomenclature": hgvs_nomenclature,
                    "genomicReferenceSequence": genomic_reference_sequence,
                    "genomicVariantSequence": genomic_variant_sequence,
                    "paddingLeft": padding_left,
                    "paddingRight": padding_right,
                    "alleleId": allele_record.get('alleleId'),
                    "dataProviders": data_providers,
                    "dateProduced": date_produced,
                    "loadKey": load_key,
                    "release": release,
                    "modGlobalCrossRefId": mod_global_cross_ref_id,
                    "dataProvider": data_provider,
                    "variantHGVSSynonym": hgvs_synonym}

                variant_genomic_location_dataset = {
                    "variantId": hgvs_nomenclature,
                    "assembly": allele_record.get('assembly'),
                    "chromosome": chromosome_str,
                    "start": allele_record.get('start'),
                    "end": allele_record.get('end'),
                    "uuid": str(uuid.uuid4()),
                    "dataProvider": data_provider}

                variant_so_term = {
                    "variantId": hgvs_nomenclature,
                    "soTermId": allele_record.get('type')}

                variant_so_terms.append(variant_so_term)
                variant_genomic_locations.append(variant_genomic_location_dataset)
                variants.append(variant_dataset)

            if counter == batch_size:
                yield [variants, variant_genomic_locations, variant_so_terms, cross_references]
                variants = []
                variant_genomic_locations = []
                variant_so_terms = []
                cross_references = []

        if counter > 0:
            yield [variants, variant_genomic_locations, variant_so_terms, cross_references]
