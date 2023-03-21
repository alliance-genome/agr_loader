"""HTP Meta Dataset Sample."""
import logging
import multiprocessing

from etl import ETL
from etl.helpers import ETLHelper
from files import JSONFile
from transactors import CSVTransactor, Neo4jTransactor
import uuid

logger = logging.getLogger(__name__)


class HTPMetaDatasetSampleETL(ETL):
    """HTP Meta Data."""

    htp_dataset_sample_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (o:OBITerm {primaryKey:row.sampleType})
                MATCH (s:Species {primaryKey:row.taxonId})
                MATCH (a:MMOTerm {primaryKey:row.assayType})

                MERGE (ds:HTPDatasetSample {primaryKey:row.datasetSampleId})
                ON CREATE SET ds.dateAssigned = row.dateAssigned,
                    ds.abundance = row.abundance,
                    ds.sex = row.sex,
                    ds.notes = row.notes,
                    ds.dateAssigned = row.dateAssigned,
                    ds.biosampleText = row.biosampleText,
                    ds.sequencingFormat = row.sequencingFormat,
                    ds.title = row.sampleTitle,
                    ds.sampleAge = row.sampleAge,
                    ds.sampleId = row.sampleId

                MERGE (ds)-[dssp:FROM_SPECIES]-(s)
                MERGE (ds)-[dsat:ASSAY_TYPE]-(a)
                MERGE (ds)-[dsst:SAMPLE_TYPE]-(o)

            }
        IN TRANSACTIONS of %s ROWS"""

    htp_dataset_sample_agm_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ds:HTPDatasetSample {primaryKey:row.datasetSampleId})
                MATCH (agm:AffectedGenomicModel {primaryKey:row.biosampleId})

                MERGE (agm)-[agmds:ASSOCIATION]-(ds)

            }
        IN TRANSACTIONS of %s ROWS"""

    htp_dataset_sample_agmtext_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ds:HTPDatasetSample {primaryKey:row.datasetSampleId})
                MERGE (agm:AffectedGenomicModel {primaryKey:row.biosampleText})

                MERGE (agm)-[agmds:ASSOCIATION]-(ds)

            }
        IN TRANSACTIONS of %s ROWS"""

    htp_bio_entity_expression_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (dss:HTPDatasetSample {primaryKey:row.datasetSampleId})

                MERGE (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                        ON CREATE SET e.whereExpressedStatement = row.whereExpressedStatement

                MERGE (dss)-[dsdss:STRUCTURE_SAMPLED]-(e)

            }
        IN TRANSACTIONS of %s ROWS"""

    htp_stages_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (dss:HTPDatasetSample {primaryKey:row.datasetSampleId})
                MATCH (st:Stage {primaryKey:row.stageName})

                MERGE (dss)-[eotcctq:SAMPLED_DURING]-(s)

            }
        IN TRANSACTIONS of %s ROWS"""

    htp_dataset_join_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ds:HTPDataset {primaryKey:row.datasetId})
                MATCH (dss:HTPDatasetSample {primaryKey:row.datasetSampleId})

                MERGE (ds)-[dsdss:ASSOCIATION]-(dss)

            }
        IN TRANSACTIONS of %s ROWS"""

    htp_secondaryIds_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (dss:HTPDatasetSample {primaryKey: row.datasetSampleId})

                MERGE (sec:SecondaryId {primaryKey:row.secondaryId})
                        ON CREATE SET sec.name = row.secondaryId

                MERGE (dss)<-[aka:ALSO_KNOWN_AS]-(sec)


            }
        IN TRANSACTIONS of %s ROWS"""

    ao_substructures_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otasst)
                        AND NOT 'FBCVTerm' in LABELS(otasst)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotasst:ANATOMICAL_SUBSTRUCTURE]->(otasst)

            }
        IN TRANSACTIONS of %s ROWS"""

    ao_qualifiers_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasst:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otasst)
                        AND NOT 'FBCVTerm' in LABELS(otasst)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotasst:ANATOMICAL_SUBSTRUCTURE]->(otasst)


            }
        IN TRANSACTIONS of %s ROWS"""

    ao_ss_qualifiers_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otasst)
                        AND NOT 'FBCVTerm' in LABELS(otasst)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotasst:ANATOMICAL_SUBSTRUCTURE]->(otasst)


            }
        IN TRANSACTIONS of %s ROWS"""

    ao_terms_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasst:Ontology {primaryKey:row.anatomicalStructureTermId})
                    WHERE NOT 'FBCVTerm' in LABELS(otasst)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotasst:ANATOMICAL_SUBSTRUCTURE]->(otasst)
            }
        IN TRANSACTIONS of %s ROWS"""

    cc_term_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasst:Ontology {primaryKey:row.cellularComponentTermId})
                    WHERE NOT 'FBCVTerm' in LABELS(otasst)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotasst:ANATOMICAL_SUBSTRUCTURE]->(otasst)
            }
        IN TRANSACTIONS of %s ROWS"""

    eas_substructure_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasst:Ontology {primaryKey:row.anatomicalSubStructureTermId})
                    WHERE NOT 'FBCVTerm' in LABELS(otasst)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotasst:ANATOMICAL_SUB_SUBSTRUCTURE]->(otasst)
            }
        IN TRANSACTIONS of %s ROWS"""

    eas_qualified_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otastq:Ontology {primaryKey:row.anatomicalStructureQualifierTermId})
                    WHERE NOT 'FBCVTerm' in LABELS(otastq)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MERGE (e)-[eotastq:ANATOMICAL_STRUCTURE_QUALIFIER]-(otastq)
            }
        IN TRANSACTIONS of %s ROWS"""

    eass_qualified_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otasstq:Ontology {primaryKey:row.anatomicalSubStructureQualifierTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otasstq)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})

                MERGE (e)-[eotasstq:ANATOMICAL_SUB_STRUCTURE_QUALIFIER]-(otasstq)
            }
        IN TRANSACTIONS of %s ROWS"""

    ccq_expression_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (otcctq:Ontology {primaryKey:row.cellularComponentQualifierTermId})
                    WHERE NOT 'UBERONTerm' in LABELS(otcctq)
                MATCH (e:ExpressionBioEntity {primaryKey:row.ebe_uuid})

                MERGE (e)-[eotcctq:CELLULAR_COMPONENT_QUALIFIER]-(otcctq)
            }
        IN TRANSACTIONS of %s ROWS"""

    stage_expression_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MERGE (s:Stage {primaryKey:row.stageName})
                    ON CREATE SET s.name = row.stageName
                MERGE (ei)-[eotcctq:DURING]-(s)
            }
        IN TRANSACTIONS of %s ROWS"""

    uberon_ao_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MATCH (o:Ontology:UBERONTerm {primaryKey:row.aoUberonId})
                MERGE (ebe)-[ebeo:ANATOMICAL_RIBBON_TERM]-(o)
            }
        IN TRANSACTIONS of %s ROWS"""

    uberon_stage_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MATCH (o:Ontology:UBERONTerm {primaryKey:row.uberonStageId})

                MERGE (ei)-[eio:STAGE_RIBBON_TERM]-(o)
            }
        IN TRANSACTIONS of %s ROWS"""

    uberon_ao_other_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ebe:ExpressionBioEntity {primaryKey:row.ebe_uuid})
                MATCH (u:Ontology:UBERONTerm:Ontology {primaryKey:'UBERON:AnatomyOtherLocation'})
                MERGE (ebe)-[ebeu:ANATOMICAL_RIBBON_TERM]-(u)
            }
        IN TRANSACTIONS of %s ROWS"""

    uberon_stage_other_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ei:BioEntityGeneExpressionJoin {primaryKey:row.ei_uuid})
                MATCH (u:Ontology:UBERONTerm:Ontology {primaryKey:'UBERON:PostEmbryonicPreAdult'})

                MERGE (ei)-[eiu:STAGE_RIBBON_TERM]-(u)
            }
        IN TRANSACTIONS of %s ROWS"""

    htp_dataset_sample_assemblies_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (ds:HTPDatasetSample {primaryKey:row.datasetSampleId})
                MATCH (u:Assembly {primaryKey:row.assembly})

                CREATE (ds)<-[dsu:ASSEMBLY]-(u)
            }
        IN TRANSACTIONS of %s ROWS"""

    htpdatasetsample_xrefs_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row
            MATCH (o:HTPDatasetSample {primaryKey:row.datasetId}) 
            """ + ETLHelper.get_cypher_xref_text() + """
            }
        IN TRANSACTIONS of %s ROWS"""

    def __init__(self, config):
        """Initialise."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        """Load and process data."""
        thread_pool = []

        for sub_type in self.data_type_config.get_sub_type_objects():
            p = multiprocessing.Process(target=self._process_sub_type, args=(sub_type,))
            p.start()
            thread_pool.append(p)

        ETL.wait_for_threads(thread_pool)

    def _process_sub_type(self, sub_type):
        """Process sub type."""
        logger.info("Loading HTP metadata sample data: %s" % sub_type.get_data_provider())
        filepath = sub_type.get_filepath()
        logger.info(filepath)
        data = JSONFile().get_data(filepath)

        logger.info("Finished Loading HTP metadata sample data: %s" % sub_type.get_data_provider())

        if data is None:
            logger.warn("No Data found for %s skipping" % sub_type.get_data_provider())
            return
        ETLHelper.load_release_info(data, sub_type, self.logger)

        # This order is the same as the lists yielded from the get_generators function.
        # A list of tuples.

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = 25000

        # This needs to be in this format (template, param1, params2) others will be ignored
        query_list = [
            [HTPMetaDatasetSampleETL.htp_dataset_sample_query_template, "htp_metadataset_sample_samples_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.htp_bio_entity_expression_query_template, "htp_metadataset_sample_bioentities_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.htp_secondaryIds_query_template, "htp_metadataset_sample_secondaryIds_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.htp_dataset_join_query_template, "htp_metadataset_sample_datasets_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.htp_stages_query_template, "htp_metadataset_sample_stages_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.ao_terms_query_template, "htp_metadataset_sample_aoterms_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.ao_substructures_query_template, "htp_metadataset_sample_ao_substructures_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.ao_qualifiers_query_template, "htp_metadataset_sample_ao_qualifiers_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.ao_ss_qualifiers_query_template, "htp_metadataset_sample_ao_ss_qualifiers_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.cc_term_query_template, "htp_metadataset_sample_ccterms" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.ccq_expression_query_template, "htp_metadataset_sample_ccqterms_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.uberon_ao_query_template, "htp_metadataset_sample_uberon_ao_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.uberon_ao_other_query_template, "htp_metadataset_sample_uberon_ao_other_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.htp_dataset_sample_agm_query_template, "htp_metadataset_sample_agms_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.htp_dataset_sample_agmtext_query_template, "htp_metadataset_sample_agmstext_" + sub_type.get_data_provider() + ".csv", commit_size],

            [HTPMetaDatasetSampleETL.htp_dataset_sample_assemblies_query_template, "htp_metadataset_sample_assemblies_" + sub_type.get_data_provider() + ".csv", commit_size]
        ]


        # Obtain the generator
        generators = self.get_generators(data, batch_size)

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    def get_generators(self, htp_datasetsample_data, batch_size):
        """Get the generator."""
        htp_datasetsamples = []
        secondaryIds = []
        datasetIDs = []
        assemblies = []
        uberon_ao_data = []
        ao_qualifiers = []
        bio_entities = []
        ao_ss_qualifiers = []
        ao_substructures = []
        ao_terms = []
        uberon_ao_other_data = []
        stages = []
        ccq_components = []
        cc_components = []
        biosamples = []
        biosamplesTexts = []
        counter = 0

        for datasample_record in htp_datasetsample_data['data']:

            counter = counter + 1
            sampleTitle = ''
            sampleId = ''
            biosampleId = ''
            biosampleText = {}
            datasetSampleId = str(uuid.uuid4())

            if 'sampleTitle' in datasample_record:
                sampleTitle = datasample_record.get('sampleTitle')

            if 'sampleId' in datasample_record:
                sampleIdObj = datasample_record.get('sampleId')
                sampleId = sampleIdObj.get('primaryId')

            if 'datasetIds' in datasample_record:
                datasetIdSet = datasample_record.get('datasetIds')
                for datasetID in datasetIdSet:

                    if self.test_object.using_test_data() is True:
                        is_it_test_entry = self.test_object.check_for_test_id_entry(datasetID)
                        if is_it_test_entry is False:
                            counter = counter - 1
                            continue
                    datasetsample = {
                        "datasetSampleId": datasetSampleId,
                        "datasetId": datasetID
                    }
                    datasetIDs.append(datasetsample)

            if 'genomicInformation' in datasample_record:
                genomicInformation = datasample_record.get('genomicInformation')
                if 'biosampleId' in genomicInformation:
                    biosampleId = genomicInformation.get('biosampleId')
                if 'bioSampleText' in genomicInformation:
                    biosampleText = genomicInformation.get('bioSampleText')

                if biosampleId is not None and biosampleId != '':
                    biosample = {
                        "biosampleId": biosampleId,
                        "datasetSampleId": datasetSampleId
                    }
                    biosamples.append(biosample)

                if biosampleText is not None and biosampleText != '' and biosampleId == '':
                    biosampleText = {
                        "biosampleText": biosampleText,
                        "datasetSampleId": datasetSampleId
                    }
                    biosamplesTexts.append(biosampleText)

            if 'assemblyVersions' in datasample_record:
                for assembly in datasample_record.get('assemblyVersions'):

                    assembly_datasample = {
                            "datasetSampleId": datasetSampleId,
                            "assembly": assembly
                        }
                    assemblies.append(assembly_datasample)

            age = ''
            if 'sampleAge' in datasample_record:
                sampleAge = datasample_record.get('sampleAge')
                stageId = ''
                if 'age' in sampleAge:
                    age = sampleAge.get('age')
                    stageId = stageId + age
                if 'stage' in sampleAge:
                    stage = sampleAge.get('stage')
                    stageId = stageId + stage.get('stageName')

                    stage = {"stageId": stageId,
                             "stageTermId": stage.get('stageTermId'),
                             "stageName": stage.get('stageName'),
                             "stageUberonSlimTerm": stage.get('stageUberonSlimTerm'),
                             "sampleAge": age,
                             "datasetSampleId": datasetSampleId}
                    stages.append(stage)
                else:
                    stage = {"stageId": stageId,
                             "sampleAge": age}
                    stages.append(stage)

            if 'sampleLocations' in datasample_record:
                sampleLocations = datasample_record.get('sampleLocations')

                for location in sampleLocations:

                    cellular_component_qualifier_term_id = location.get('cellularComponentQualifierTermId')
                    cellular_component_term_id = location.get('cellularComponentTermId')
                    anatomical_structure_term_id = location.get('anatomicalStructureTermId')
                    anatomical_structure_qualifier_term_id = location.get('anatomicalStructureQualifierTermId')
                    anatomical_sub_structure_term_id = location.get('anatomicalSubStructureTermId')
                    anatomical_sub_structure_qualifier_term_id = location.get('anatomicalSubStructureQualifierTermId')
                    where_expressed_statement = location.get('whereExpressedStatement')

                    expression_unique_key = datasetSampleId
                    expression_entity_unique_key = ''

                    if anatomical_structure_term_id is not None:
                        expression_unique_key += anatomical_structure_term_id
                        expression_entity_unique_key = anatomical_structure_term_id

                        if anatomical_structure_qualifier_term_id is not None:
                            expression_unique_key += anatomical_structure_qualifier_term_id
                            expression_entity_unique_key += anatomical_structure_qualifier_term_id

                    if cellular_component_term_id is not None:
                        expression_unique_key += cellular_component_term_id
                        expression_entity_unique_key += cellular_component_term_id

                        if cellular_component_qualifier_term_id is not None:
                            expression_unique_key += cellular_component_qualifier_term_id
                            expression_entity_unique_key += cellular_component_qualifier_term_id

                    if anatomical_sub_structure_term_id is not None:
                        expression_unique_key += anatomical_sub_structure_term_id

                        if anatomical_sub_structure_qualifier_term_id is not None:
                            expression_unique_key += anatomical_sub_structure_qualifier_term_id
                            expression_entity_unique_key += anatomical_sub_structure_qualifier_term_id

                    expression_entity_unique_key += where_expressed_statement

                    if location.get('anatomicalStructureUberonSlimTermIds') is not None:

                        for uberon_structure_term_object in location.get('anatomicalStructureUberonSlimTermIds'):
                            structure_uberon_term_id = uberon_structure_term_object.get('uberonTerm')

                            if structure_uberon_term_id is not None and structure_uberon_term_id != 'Other':
                                structure_uberon_term = {
                                    "ebe_uuid": expression_entity_unique_key,
                                    "aoUberonId": structure_uberon_term_id}
                                uberon_ao_data.append(structure_uberon_term)

                            elif structure_uberon_term_id is not None and structure_uberon_term_id == 'Other':
                                other_structure_uberon_term = {
                                    "ebe_uuid": expression_entity_unique_key}
                                uberon_ao_other_data.append(other_structure_uberon_term)

                    if location.get('anatomicalSubStructureUberonSlimTermIds') is not None:

                        for uberon_sub_structure_term_object in location.get('anatomicalSubStructureUberonSlimTermIds'):
                            sub_structure_uberon_term_id = uberon_sub_structure_term_object.get('uberonTerm')

                            if sub_structure_uberon_term_id is not None and sub_structure_uberon_term_id != 'Other':
                                sub_structure_uberon_term = {
                                    "ebe_uuid": expression_entity_unique_key,
                                    "aoUberonId": sub_structure_uberon_term_id}
                                uberon_ao_data.append(sub_structure_uberon_term)

                            elif sub_structure_uberon_term_id is not None and sub_structure_uberon_term_id == 'Other':
                                other_structure_uberon_term = {
                                    "ebe_uuid": expression_entity_unique_key}
                                uberon_ao_other_data.append(other_structure_uberon_term)

                    if cellular_component_term_id is not None:
                        cc_term = {
                            "ebe_uuid": expression_entity_unique_key,
                            "cellularComponentTermId": cellular_component_term_id
                        }
                        cc_components.append(cc_term)

                    if cellular_component_qualifier_term_id is not None:
                        ccq_term = {
                            "ebe_uuid": expression_entity_unique_key,
                            "cellularComponentQualifierTermId": cellular_component_qualifier_term_id
                        }
                        ccq_components.append(ccq_term)

                    if anatomical_structure_term_id is not None:
                        ao_term = {
                            "ebe_uuid": expression_entity_unique_key,
                            "anatomicalStructureTermId": anatomical_structure_term_id
                        }
                        ao_terms.append(ao_term)

                    if anatomical_structure_qualifier_term_id is not None:
                        ao_qualifier = {
                                "ebe_uuid": expression_entity_unique_key,
                                "anatomicalStructureQualifierTermId": anatomical_structure_qualifier_term_id
                        }

                        ao_qualifiers.append(ao_qualifier)

                    if anatomical_sub_structure_term_id is not None:
                        ao_substructure = {
                                "ebe_uuid": expression_entity_unique_key,
                                "anatomicalSubStructureTermId": anatomical_sub_structure_term_id
                        }

                        ao_substructures.append(ao_substructure)

                    if anatomical_sub_structure_qualifier_term_id is not None:
                        ao_ss_qualifier = {
                                "ebe_uuid": expression_entity_unique_key,
                                "anatomicalSubStructureQualifierTermId": anatomical_sub_structure_qualifier_term_id}

                        ao_ss_qualifiers.append(ao_ss_qualifier)

                    if where_expressed_statement is None:
                        where_expressed_statement = ""

                    bio_entity = {
                        "ebe_uuid": expression_entity_unique_key,
                        "whereExpressedStatement": where_expressed_statement,
                        "datasetSampleId": datasetSampleId
                    }
                    bio_entities.append(bio_entity)

            # TODO: remove when WB corrects their taxon submission
            taxonId = datasample_record.get('taxonId')

            htp_dataset_sample = {
                "datasetSampleId": datasetSampleId,
                "abundance": datasample_record.get('abundance'),
                "sampleType": datasample_record.get('sampleType'),
                "taxonId": taxonId,
                "sex": datasample_record.get('sex'),
                "assayType": datasample_record.get('assayType'),
                "notes": datasample_record.get('notes'),
                "dateAssigned": datasample_record.get('dateAssigned'),
                "sequencingFormat": datasample_record.get('sequencingFormat'),
                "sampleTitle": sampleTitle,
                "sampleAge": age,
                "sampleId": sampleId
            }

            htp_datasetsamples.append(htp_dataset_sample)

            if counter == batch_size:
                yield [htp_datasetsamples,
                       bio_entities,
                       secondaryIds,
                       datasetIDs,
                       stages,
                       ao_terms,
                       ao_substructures,
                       ao_qualifiers,
                       ao_ss_qualifiers,
                       cc_components,
                       ccq_components,
                       uberon_ao_data,
                       uberon_ao_other_data,
                       biosamples,
                       biosamplesTexts,
                       assemblies
                       ]
                counter = 0
                htp_datasetsamples = []
                datasetIDs = []
                uberon_ao_data = []
                ao_qualifiers = []
                bio_entities = []
                ao_ss_qualifiers = []
                ao_substructures = []
                ao_terms = []
                uberon_ao_other_data = []
                stages = []
                ccq_components = []
                cc_components = []
                biosamples = []
                biosamplesTexts = []
                assemblies = []

        if counter > 0:
            yield [htp_datasetsamples,
                   bio_entities,
                   secondaryIds,
                   datasetIDs,
                   stages,
                   ao_terms,
                   ao_substructures,
                   ao_qualifiers,
                   ao_ss_qualifiers,
                   cc_components,
                   ccq_components,
                   uberon_ao_data,
                   uberon_ao_other_data,
                   biosamples,
                   biosamplesTexts,
                   assemblies
                   ]
