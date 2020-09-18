"""Variation ETL."""

import logging
import multiprocessing
import uuid

from etl import ETL
from etl.helpers import ETLHelper, AssemblySequenceHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor
from data_manager import DataFileManager
from loader_common import ContextInfo


class VariationETL(ETL):
    """Variation ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    variation_query_template = """
            USING PERIODIC COMMIT %s
            LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

                MATCH (a:Allele {primaryKey:row.alleleId})
                MATCH (g:Gene)-[:IS_ALLELE_OF]-(a)

                //Create the variant node and set properties. primaryKey is required.
                MERGE (o:Variant {primaryKey:row.hgvs_nomenclature})
                    ON CREATE SET
                     o.name = row.variantHGVSSynonym,
                     o.hgvsNomenclature = row.hgvs_nomenclature,
                     o.genomicReferenceSequence = row.genomicReferenceSequence,
                     o.paddingLeft = row.paddingLeft,
                     o.paddingRight = row.paddingRight,
                     o.genomicVariantSequence = row.genomicVariantSequence,
                     o.dateProduced = row.dateProduced,
                     o.release = row.release,
                     o.dataProviders = row.dataProviders,
                     o.dataProvider = row.dataProvider

                MERGE (s:Synonym:Identifier {primaryKey:row.hgvs_nomenclature})
                    SET s.name = row.hgvs_nomenclature
                MERGE (o)-[aka2:ALSO_KNOWN_AS]->(s)

                MERGE (o)-[:VARIATION]->(a)
                MERGE (g)-[:COMPUTED_GENE]->(o) """

    so_terms_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:Variant {primaryKey:row.variantId})
            MATCH (s:SOTerm {primaryKey:row.soTermId})
            CREATE (o)-[:VARIATION_TYPE]->(s)"""

    pubs_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            MATCH (o:Variant {primaryKey:row.variantId})
            MERGE (p:Publication {primaryKey:row.publicationId})
              ON CREATE SET p.pubModId = row.pubModId,
                 p.pubMedId = row.pubMedId,
                 p.pubModUrl = row.pubModUrl,
                 p.pubMedUrl = row.pubMedUrl

           MERGE (o)-[pn:ASSOCIATION]-(p)
            """

    genomic_locations_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Variant {primaryKey:row.variantId})
            MATCH (chrm:Chromosome {primaryKey:row.chromosome})
            MERGE (a:Assembly {primaryKey:row.assembly})
             ON CREATE SET a.dataProvider = row.dataProvider

            CREATE (o)-[gchrm:LOCATED_ON]->(chrm)

            CREATE (gchrmn:GenomicLocation {primaryKey:row.uuid})
              SET gchrmn.start = apoc.number.parseInt(row.start),
                gchrmn.end = apoc.number.parseInt(row.end),
                gchrmn.assembly = row.assembly,
                gchrmn.strand = row.strand,
                gchrmn.chromosome = row.chromosome

            CREATE (o)-[of:ASSOCIATION]->(gchrmn)
            CREATE (gchrmn)-[ofc:ASSOCIATION]->(chrm)
            CREATE (gchrmn)-[ao:ASSOCIATION]->(a)
    """
    notes_query_template = """
           USING PERIODIC COMMIT %s
           LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

           MATCH (o:Variant {primaryKey:row.variantId})

           CREATE (n:Note {primaryKey:row.noteId})
                SET n.note = row.note          
           MERGE (o)-[pn:ASSOCIATION]-(n)
    """
    notes_references_query_template = """
           USING PERIODIC COMMIT %s
           LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

           MATCH (o:Note {primaryKey:row.noteId})
           MERGE (p:Publication {primaryKey:row.publicationId})
              ON CREATE SET p.pubModId = row.pubModId,
                 p.pubMedId = row.pubMedId,
                 p.pubModUrl = row.pubModUrl,
                 p.pubMedUrl = row.pubMedUrl

           MERGE (o)-[pn:ASSOCIATION]-(p)
    """

    variant_xrefs_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (o:Variant {primaryKey:row.dataId}) 

    """ + ETLHelper.get_cypher_xref_text()

    def __init__(self, config):
        """Initialise object."""
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

        self.logger.info("Loading Variation Data: %s", sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        data = JSONFile().get_data(filepath)
        self.logger.info("Finished Loading Variation Data: %s", sub_type.get_data_provider())

        if data is None:
            self.logger.warning("No Data found for %s skipping", sub_type.get_data_provider())
            return

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # This needs to be in this format (template, param1, params2) others will be ignored

        query_template_list = [
            [self.variation_query_template, commit_size,
             "variation_data_" + sub_type.get_data_provider() + ".csv"],
            [self.genomic_locations_query_template, commit_size,
             "variant_genomiclocations_" + sub_type.get_data_provider() + ".csv"],
            [self.so_terms_query_template, commit_size,
             "variant_so_terms_" + sub_type.get_data_provider() + ".csv"],
            [self.notes_query_template, commit_size,
             "variant_notes_" + sub_type.get_data_provider() + ".csv"],
            [self.notes_references_query_template, commit_size,
             "variant_notes_references" + sub_type.get_data_provider() + ".csv"],
            [self.pubs_query_template, commit_size,
             "variant_references" + sub_type.get_data_provider() + ".csv"],
            [self.variant_xrefs_query_template, commit_size,
             "variant_xrefs_" + sub_type.get_data_provider() + ".csv"],
        ]

        generators = self.get_generators(data, batch_size)
        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("Var-{}: ".format(sub_type.get_data_provider()))

    def get_hgvs_nomenclature(self, refseq_id, variant_type, start_position,
                              end_position, reference_sequence, variant_sequence,
                              assembly, chromosome):
        """Get HGVS nomenclature."""
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

        hgvs_nomenclature = refseq_id.split(":")[1] + ':g.' + start_position_str
        hgvs_synonym = '(' + assembly + ')' + chromosome + ':' + start_position_str

        if variant_type in ['SO:1000002', 'SO:1000008']:  # point mutation/substitution
            hgvs_nomenclature += reference_sequence_str + ">" + variant_sequence_str
            hgvs_synonym += reference_sequence_str + ">" + variant_sequence_str
        elif variant_type == "SO:0000667":  # insertion
            hgvs_nomenclature += '_' + end_position_str + 'ins' + variant_sequence_str
            hgvs_synonym += '_' + end_position_str + 'ins' + variant_sequence_str
        elif variant_type == "SO:0000159":  # deletion
            hgvs_nomenclature += '_' + end_position_str + 'del'
            hgvs_synonym += '_' + end_position_str + 'del'
        elif variant_type == "SO:0002007":  # MNV
            hgvs_nomenclature += '_' + end_position_str + 'delins' + variant_sequence_str
            hgvs_synonym += '_' + end_position_str + 'delins' + variant_sequence_str
        elif variant_type == "SO:1000032":  # DELIN
            hgvs_nomenclature += '_' + end_position_str + 'delins' + variant_sequence_str
            hgvs_synonym += '_' + end_position_str + 'delins' + variant_sequence_str
        else:
            hgvs_nomenclature = ''
            hgvs_synonym = ''
        return hgvs_nomenclature, hgvs_synonym

    def get_generators(self, variant_data, batch_size):
        """Get Generators."""

        data_providers = []
        release = ""
        variants = []
        variant_genomic_locations = []
        variant_so_terms = []
        variant_notes = []
        variant_note_references = []
        variant_pubs = []
        cross_references = []
        counter = 0

        date_produced = variant_data['metaData']['dateProduced']

        self.data_providers_process(variant_data)
        load_key = date_produced + self.data_provider + "_VARIATION"

        if 'release' in variant_data['metaData']:
            release = variant_data['metaData']['release']

        assemblies = {}
        for allele_record in variant_data['data']:

            # TODO: fix typo in MGI Submission for this variant so
            # that it doesn't list a 40K bp point mutation.
            if allele_record.get('alleleId') == 'MGI:6113870':
                continue

            counter = counter + 1
            global_id = allele_record.get('alleleId')
            mod_global_cross_ref_id = ""

            chromosome = allele_record["chromosome"]
            if chromosome.startswith("chr"):
                chromosome_str = chromosome[3:]
            else:
                chromosome_str = chromosome

            assembly = allele_record["assembly"]

            if assembly not in assemblies:
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
                    genomic_reference_sequence = assemblies[assembly].get_sequence(
                        chromosome_str,
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

            if self.test_object.using_test_data() is True:
                is_it_test_entry = self.test_object.check_for_test_id_entry(global_id)
                if is_it_test_entry is False:
                    counter = counter - 1
                    continue

            cross_ref_primary_id = allele_record.get('sequenceOfReferenceAccessionNumber')
            local_cross_ref_id = cross_ref_primary_id.split(":")[1]
            prefix = cross_ref_primary_id.split(":")[0]

            cross_ref_complete_url = self.etlh.get_no_page_complete_url(
                            local_cross_ref_id, prefix, global_id)
            xref_map_acc = ETLHelper.get_xref_dict(
                            local_cross_ref_id,
                            prefix,
                            "variant_sequence_of_reference",
                            "sequence_of_reference_accession_number",
                            global_id,
                            cross_ref_complete_url,
                            cross_ref_primary_id + "variant_sequence_of_reference")

            xref_map_acc['dataId'] = global_id

            if cross_ref_primary_id is not None:
                cross_references.append(xref_map_acc)

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

            if 'crossReferences' in allele_record:
                for cross_ref in allele_record.get('crossReferences'):
                    cross_ref_id = cross_ref.get('id')
                    local_cross_ref_id = cross_ref_id.split(":")[1]
                    prefix = cross_ref.get('id').split(":")[0]
                    pages = cross_ref.get('pages')
                    global_xref_id = cross_ref.get('id')

                    # some pages collection have 0 elements
                    if pages is not None and len(pages) > 0:
                        for page in pages:
                            display_name = ""

                            cross_ref_complete_url = self.etlh.rdh2.return_url_from_key_value(
                                prefix, local_cross_ref_id, page)

                            xref_map = ETLHelper.get_xref_dict(
                                local_cross_ref_id,
                                prefix,
                                page,
                                page,
                                display_name,
                                cross_ref_complete_url,
                                global_xref_id + page)
                            xref_map['dataId'] = hgvs_nomenclature
                            cross_references.append(xref_map)

            if (genomic_reference_sequence is not None and len(genomic_reference_sequence) > 30000) \
                    or (genomic_variant_sequence is not None and len(genomic_variant_sequence)) > 30000:
                self.logger.debug("%s has too long of a sequence potentionally",
                                  allele_record.get('alleleId'))

            references = allele_record.get('references')
            if references is not None:
                for evidence in references:
                    pub_med_url = None
                    pub_med_id = ""
                    pub_mod_url = None
                    publication_mod_id = ""

                    if 'publicationId' in evidence:
                        publication = evidence.get('publicationId')
                        # WB has an error in their pubs at the moment, fix/remove 'and' stanza here, when we
                        # have a new file.
                        if publication.startswith('PMID:') and publication != 'PMID:':
                            pub_med_id = publication
                            pub_med_url = self.etlh.return_url_from_identifier(pub_med_id)
                            if 'crossReference' in evidence:
                                pub_xref = evidence.get('crossReference')
                                publication_mod_id = pub_xref.get('id')
                                pub_mod_url = self.etlh.return_url_from_identifier(publication_mod_id)
                        else:
                            publication_mod_id = publication
                            pub_mod_url = self.etlh.return_url_from_identifier(publication_mod_id)

                    variant_pub = {
                        "publicationId": publication_mod_id + pub_med_id,
                        "pubModId": publication_mod_id,
                        "pubMedId": pub_med_id,
                        "pubMedUrl": pub_med_url,
                        "pubModUrl": pub_mod_url,
                        "variantId": hgvs_nomenclature

                    }
                    variant_pubs.append(variant_pub)

            notes = allele_record.get('notes')

            if notes is not None:
                for note in notes:
                    vnote = note.get('note')
                    note_id = str(uuid.uuid4())

                    variant_note_map = {"variantId": hgvs_nomenclature,
                                            "noteId": note_id,
                                            "note": vnote}

                    variant_notes.append(variant_note_map)

                    vreferences = note.get('references')
                    if vreferences is not None:
                        for evidence in vreferences:
                            pub_med_url = None
                            pub_med_id = ""
                            pub_mod_url = None
                            publication_mod_id = ""

                            if 'publicationId' in evidence:
                                publication = evidence.get('publicationId')
                                if publication.startswith('PMID:'):
                                    pub_med_id = publication
                                    pub_med_url = self.etlh.return_url_from_identifier(pub_med_id)
                                    if 'crossReference' in evidence:
                                        pub_xref = evidence.get('crossReference')
                                        publication_mod_id = pub_xref.get('id')
                                        pub_mod_url = self.etlh.return_url_from_identifier(publication_mod_id)
                                else:
                                    publication_mod_id = publication
                                    pub_mod_url = self.etlh.return_url_from_identifier(publication_mod_id)

                                note_pub = {"publicationId": publication_mod_id + pub_med_id,
                                            "pubModId": publication_mod_id,
                                            "pubMedId": pub_med_id,
                                            "pubMedUrl": pub_med_url,
                                            "pubModUrl": pub_mod_url,
                                            "noteId": note_id
                                }
                                variant_note_references.append(note_pub)

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
                "dataProvider": self.data_provider,
                "variantHGVSSynonym": hgvs_synonym}

            variant_genomic_location_dataset = {
                    "variantId": hgvs_nomenclature,
                    "assembly": allele_record.get('assembly'),
                    "chromosome": chromosome_str,
                    "start": allele_record.get('start'),
                    "end": allele_record.get('end'),
                    "uuid": str(uuid.uuid4()),
                    "dataProvider": self.data_provider}

            variant_so_term = {
                    "variantId": hgvs_nomenclature,
                    "soTermId": allele_record.get('type')}

            variant_so_terms.append(variant_so_term)
            variant_genomic_locations.append(variant_genomic_location_dataset)

            variants.append(variant_dataset)

            if counter == batch_size:
                yield [variants,
                       variant_genomic_locations, variant_so_terms,
                       variant_notes, variant_note_references, variant_pubs,
                       cross_references]
                variants = []
                variant_genomic_locations = []
                variant_so_terms = []
                variant_notes = []
                variant_note_references = []
                variant_pubs = []
                cross_references = []

        if counter > 0:
            yield [variants,
                   variant_genomic_locations, variant_so_terms,
                    variant_notes, variant_note_references, variant_pubs,
                   cross_references]


