"""Species ETL"""

import logging
import yaml
from etl import ETL
from transactors import CSVTransactor
from transactors import Neo4jTransactor
from files import Download


class SpeciesETL(ETL):
    """Species ETL"""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    main_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MERGE (s:Species {primaryKey: row.taxon_id})
          ON CREATE SET s.shortName = row.short_name,
                        s.species = row.short_name,
                        s.name = row.name,
                        s.dataProviderFullName = row.data_provider_full_name,
                        s.dataProviderShortName = row.data_provider_short_name,
                        s.phylogeneticOrder = apoc.number.parseInt(row.phylogenic_order),
                        s.commonNames = row.common_names

        """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config


    def _load_and_process_data(self):

        filepath = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/species/species.yaml'
        generators = self.get_generators(filepath)

        query_template_list = [[self.main_query_template, 10000, "species_data.csv"]]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)


    def get_generators(self, filepath):
        """Get Generators"""


        species_file = Download('tmp', filepath, 'species.yaml').get_downloaded_data()
        yaml_list = yaml.load(species_file, Loader=yaml.SafeLoader)
        species_list = []

        for stanza in yaml_list:
            common_names = []
            for name in stanza.get("commonNames"):
                common_names.append(name)
            species_dataset = {
                "taxon_id": stanza.get("taxonId"),
                "name": stanza.get("primaryDataProvider").get("dataProviderFullName"),
                "short_name": stanza.get("shortName"),
                "common_names": common_names,
                "data_provider_full_name": stanza.get("primaryDataProvider").get("dataProviderFullName"),
                "data_provider_short_name": stanza.get("primaryDataProvider").get("dataProviderShortName"),
                "phylogenetic_order": stanza.get("phylogeneticOrder")
            }
            species_list.append(species_dataset)
        yield [species_list]
