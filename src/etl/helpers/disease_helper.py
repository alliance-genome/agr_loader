'''Disease Helper'''

import uuid
import logging

from . import ETLHelper


class DiseaseHelper():
    '''Disease Helper'''

    logger = logging.getLogger(__name__)

    @staticmethod
    def get_disease_allele(disease_record, data_providers, date_produced, data_provider_single):
        '''Get Disease Record'''

        qualifier = None
        publication_mod_id = None
        pubmed_id = None
        annotation_data_providers = []
        pge_key = ''

        primary_id = disease_record.get('objectId')

        load_key = date_produced + "_Disease"

        for data_provider in data_providers:
            load_key = data_provider + load_key

        if 'qualifier' in disease_record:
            qualifier = disease_record.get('qualifier')

        if qualifier is None:
            if 'evidence' in disease_record:

                publication_mod_id = ""
                pubmed_id = ""
                pub_mod_url = None
                pubmed_url = None
                disease_association_type = None
                ecodes = []
                annotation_uuid = str(uuid.uuid4())

                evidence = disease_record.get('evidence')
                if 'publication' in evidence:
                    publication = evidence.get('publication')
                    if publication.get('publicationId').startswith('PMID:'):
                        pubmed_id = publication.get('publicationId')
                        local_pubmed_id = pubmed_id.split(":")[1]
                        pubmed_url = ETLHelper.get_complete_pub_url(local_pubmed_id,
                                                                    pubmed_id)
                        if 'crossReference' in evidence:
                            pub_xref = evidence.get('crossReference')
                            publication_mod_id = pub_xref.get('id')
                            local_pub_mod_id = publication_mod_id.split(":")[1]
                            pub_mod_url = ETLHelper.get_complete_pub_url(local_pub_mod_id,
                                                                         publication_mod_id)
                    else:
                        publication_mod_id = publication.get('publicationId')
                        local_pub_mod_id = publication_mod_id.split(":")[1]
                        pub_mod_url = ETLHelper.get_complete_pub_url(local_pub_mod_id,
                                                                     publication_mod_id)

            if 'objectRelation' in disease_record:
                disease_association_type = disease_record['objectRelation'] \
                                            .get("associationType").upper()

                additional_genetic_components = []
                if 'additionalGeneticComponents' in disease_record['objectRelation']:
                    for component in disease_record['objectRelation'] \
                                                   ['additionalGeneticComponents']:
                        component_symbol = component.get('componentSymbol')
                        component_id = component.get('componentId')
                        component_url = component.get('componentUrl') + component_id
                        additional_genetic_components.append(
                            {"id": component_id,
                             "componentUrl": component_url,
                             "componentSymbol": component_symbol})

            if 'dataProvider' in disease_record:
                for data_provider in disease_record['dataProvider']:
                    annotation_type = data_provider.get('type')
                    xref = data_provider.get('crossReference')
                    cross_ref_id = xref.get('id')
                    pages = xref.get('pages')

                    annotation_data_provider = {"annotationType": annotation_type,
                                                "crossRefId": cross_ref_id,
                                                "dpPages": pages}
                    annotation_data_providers.append(annotation_data_provider)
            if 'evidenceCodes' in disease_record['evidence']:
                ecodes = disease_record['evidence'].get('evidenceCodes')

            do_id = disease_record.get('DOid')

            disease_unique_key = disease_record.get('objectId') + disease_record.get('DOid') + \
                                 disease_record['objectRelation'].get("associationType").upper()

            if 'with' in disease_record:
                with_record = disease_record.get('with')
                for rec in with_record:
                    disease_unique_key = disease_unique_key + rec

            if 'primaryGeneticEntityIDs' in disease_record:
                pge_ids = disease_record.get('primaryGeneticEntityIDs')
                for pge in pge_ids:
                    pge_key = pge_key + pge

            else:
                pge_ids = []

            disease_allele = {
                "diseaseUniqueKey": disease_unique_key,
                "doId": do_id,
                "primaryId": primary_id,
                "pecjPrimaryKey": annotation_uuid,
                "dataProviders": data_providers,
                "relationshipType": disease_association_type.upper(),
                "dateProduced": date_produced,
                "dataProvider": data_provider_single,
                "dateAssigned": disease_record["dateAssigned"],
                "pubPrimaryKey": publication_mod_id + pubmed_id,
                "pubModId": publication_mod_id,
                "pubMedId": pubmed_id,
                "pubMedUrl": pubmed_url,
                "pubModUrl": pub_mod_url,
                "pgeIds": pge_ids,
                "pgeKey": pge_key,
                "annotationDP": annotation_data_providers,
                "ecodes": ecodes}

            return disease_allele
