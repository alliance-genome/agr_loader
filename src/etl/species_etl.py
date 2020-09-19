"""Species ETL."""

import logging
import yaml
from etl import ETL
from transactors import CSVTransactor
from transactors import Neo4jTransactor
from files import Download


class SpeciesETL(ETL):
    """Species ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    main_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        CREATE (s:Species {primaryKey: row.taxonId})
        
          SET s.shortName = row.shortName,
              s.species = row.shortName,
              s.name = row.name,
              s.dataProviderFullName = row.dataProviderFullName,
              s.dataProviderShortName = row.dataProviderShortName,
              s.phylogeneticOrder = apoc.number.parseInt(row.phylogeneticOrder),
              s.commonNames = row.commonNames

        """
    synonym_query_template = """
     USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        
        MATCH (s:Species {primaryKey:row.taxonId})
        MERGE (sy:Synonym:Identifier {primaryKey:row.synonym})
          SET sy.name = row.synonym
          
        MERGE (s)-[ss:ALSO_KNOWN_AS]->(sy)
        
    
    """

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        filepath = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/species/species.yaml'
        generators = self.get_generators(filepath)

        query_template_list = [[self.main_query_template, 10000, "species_data.csv"],
                               [self.synonym_query_template, 10000, "species_synonym_data.csv"]]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages("Species: ")

    def get_generators(self, filepath):

        """Get Generators."""

        species_file = Download('tmp', filepath, 'species.yaml').get_downloaded_data()
        yaml_list = yaml.load(species_file, Loader=yaml.SafeLoader)
        species_list = []
        synonyms = []
        for stanza in yaml_list:
            common_names = []
            taxonId = stanza.get("taxonId")
            for name in stanza.get("commonNames"):
                common_names.append(name)
                synonym = {
                    "taxonId": stanza.get("taxonId"),
                    "synonym": name,
                }
                synonyms.append(synonym)
            species_dataset = {
                "taxonId": taxonId,
                "name": stanza.get("fullName"),
                "shortName": stanza.get("shortName"),
                "commonNames": common_names,
                "dataProviderFullName": stanza.get("primaryDataProvider").get("dataProviderFullName"),
                "dataProviderShortName": stanza.get("primaryDataProvider").get("dataProviderShortName"),
                "phylogeneticOrder": stanza.get("phylogenicOrder")
            }
            species_list.append(species_dataset)
        yield [species_list, synonyms]
