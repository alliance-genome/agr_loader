import logging, os, uuid
import time

from ..extractors import ResourceDescriptorExtractor
from ..test import TestObject


logger = logging.getLogger(__name__)

class ETL(object):

    xrefUrlMap = ResourceDescriptorExtractor().get_data()

    def __init__(self):

        if "TEST_SET" in os.environ and os.environ['TEST_SET'] == "True":
            logger.warn("WARNING: Test data load enabled.")
            time.sleep(1)
            self.testObject = TestObject(True)
        else:
            self.testObject = TestObject(False)

    def run_etl(self):
        self._load_and_process_data()

    @staticmethod
    def get_cypher_xref_text():
        return """
                MERGE (id:CrossReference:Identifier {primaryKey:row.primaryKey})
                    SET id.name = row.id,
                     id.globalCrossRefId = row.globalCrossRefId,
                     id.localId = row.localId,
                     id.crossRefCompleteUrl = row.crossRefCompleteUrl,
                     id.prefix = row.prefix,
                     id.crossRefType = row.crossRefType,
                     id.uuid = row.uuid,
                     id.page = row.page,
                     id.primaryKey = row.primaryKey,
                     id.displayName = row.displayName

                MERGE (o)-[gcr:CROSS_REFERENCE]->(id) """

    @staticmethod
    def get_xref_dict(localId, prefix, crossRefType, page, displayName, crossRefCompleteUrl, primaryId):
        globalXrefId = prefix+":"+localId
        crossReference = {
            "id": globalXrefId,
            "globalCrossRefId": globalXrefId,
            "localId": localId,
            "prefix": prefix,
            "crossRefType": crossRefType,
            "primaryKey": primaryId,
            "uuid":  str(uuid.uuid4()),
            "page": page,
            "displayName": displayName,
            "crossRefCompleteUrl": crossRefCompleteUrl,
            "name": globalXrefId
        }
        return crossReference

    @staticmethod
    def species_lookup_by_taxonid(taxon_id):
        if taxon_id in "NCBITaxon:7955":
            return "Danio rerio"
        elif taxon_id in "NCBITaxon:6239":
            return "Caenorhabditis elegans"
        elif taxon_id in "NCBITaxon:10090":
            return "Mus musculus"
        elif taxon_id in "NCBITaxon:10116":
            return "Rattus norvegicus"
        elif taxon_id in "NCBITaxon:559292":
            return "Saccharomyces cerevisiae"
        elif taxon_id in "taxon:559292":
            return "Saccharomyces cerevisiae"
        elif taxon_id in "NCBITaxon:7227":
            return "Drosophila melanogaster"
        elif taxon_id in "NCBITaxon:9606":
            return "Homo sapiens"
        else:
            return None

    @staticmethod
    def species_lookup_by_data_provider(provider):
        if provider in "ZFIN":
            return "Danio rerio"
        elif provider in "MGI":
            return "Mus musculus"
        elif provider in "FB":
            return "Drosophila melanogaster"
        elif provider in "RGD":
            return "Rattus norvegicus"
        elif provider in "WB":
            return "Caenorhabditis elegans"
        elif provider in "SGD":
            return "Saccharomyces cerevisiae"
        else:
            return None

    @staticmethod
    def data_provider_lookup(species):
        if species == 'Danio rerio':
            return 'ZFIN'
        elif species == 'Mus musculus':
            return 'MGI'
        elif species == 'Drosophila melanogaster':
            return 'FB'
        elif species == 'Homo sapiens':
            return 'RGD'
        elif species == 'Rattus norvegicus':
            return 'RGD'
        elif species == 'Caenorhabditis elegans':
            return 'WB'
        elif species == 'Saccharomyces cerevisiae':
            return 'SGD'
        else:
            return 'Alliance'