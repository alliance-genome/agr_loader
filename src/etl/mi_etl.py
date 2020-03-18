'''Molecular Interaction ETL'''

import logging

from etl import ETL
from etl.helpers import OBOHelper
from files import TXTFile
from transactors import CSVTransactor, Neo4jTransactor


class MIETL(ETL):
    '''MI ETL'''

    logger = logging.getLogger(__name__)

    query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        //Create the MITerm node and set properties. primaryKey is required.
        MERGE (g:MITerm:Ontology {primaryKey:row.identifier})
            SET g.label = row.label
            SET g.url = row.url
            SET g.definition = row.definition
        MERGE (g)-[ggmg:IS_A_PART_OF_CLOSURE]->(g)
    """

    def __init__(self, config):
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        filepath = self.data_type_config.get_single_filepath()
        generators = self.get_generators(filepath)

        query_list = [[MIETL.query_template, 10000, "mi_term_data.csv"]]

        query_and_file_list = self.process_query_params(query_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)


    @staticmethod
    def add_miterm_url(identifier):
        '''Add MI Term URL'''

        mi_term_url_dict = {
            'MI:0465' : 'http://dip.doe-mbi.ucla.edu/',
            'MI:0469' : 'http://www.ebi.ac.uk/intact',
            'MI:0471' : 'http://mint.bio.uniroma2.it',
            'MI:0478' : 'http://flybase.org',
            'MI:0486' : 'http://www.uniprot.org',
            'MI:0487' : 'http://www.wormbase.org/',
            'MI:0670' : 'https://www.imexconsortium.org/',
            'MI:0903' : 'https://www.ebi.ac.uk/intact/',
            'MI:0917' : 'http://matrixdb.univ-lyon1.fr/',
            'MI:0974' : 'http://www.innatedb.ca/',
            'MI:1222' : 'http://www.mechanobio.info/',
            'MI:1262' : 'http://ophid.utoronto.ca/',
            'MI:1263' : 'http://www.molecularconnections.com',
            'MI:1264' : 'http://www.ntnu.no/home',
            'MI:1335' : 'http://www.agbase.msstate.edu/hpi/main.html',
            'MI:0463' : 'https://thebiogrid.org/'
        }

        return mi_term_url_dict.get(identifier)

    @staticmethod
    def adjust_database_names(name):
        '''Adjust database names'''

        mi_database_name_dict = {
            'flybase': 'FlyBase',
            'wormbase': 'WormBase',
            'biogrid': 'BioGRID',
            'imex': 'IMEx',
            'intact': 'IntAct',
            'mint': 'MINT',
            'dip': 'DIP',
            'iid': 'IID',
            'uniprot knowledge base': 'UniProtKB',
            'ntnu': 'NTNU',
            'molecular connections': 'Molecular Connections',
            'hpidb': 'HPIDB',
            'innatedb': 'InnateDB',
            'matrixdb': 'MatrixDB',
            'mbinfo': 'MBInfo',
            'bhf-ucl': 'BHF-UCL',
            'mpidb': 'MPIDB'
        }

        # Return the original if there is no entry in the dict.
        return mi_database_name_dict.get(name, name)

    @staticmethod
    def add_definition(term):
        '''Add definition'''

        try:
            return term['annotation']['definition'][0]
        except KeyError:
            return None

    def get_generators(self, filepath):
        '''Create Genrators'''

        o_data = TXTFile(filepath).get_data()
        parsed_line = OBOHelper.parse_obo(o_data)

        processed_mi_list = []

        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.

            ident = line['id'].strip()

            definition = line.get('def')

            if definition is None:
                definition = ""
            else:
                # Looking to remove instances of \" in the definition string.
                if "\\\"" in definition:
                    # Replace them with just a single "
                    definition = definition.replace('\\\"', '\"')
                if definition is None:
                    definition = ""

            is_obsolete = line.get('is_obsolete')
            if is_obsolete is None:
                is_obsolete = "false"

            if ident is None or ident == '':
                self.logger.warning("Missing oid.")

            else:
                dict_to_append = {
                    'name': self.adjust_database_names(line.get('name')),
                    'name_key': self.adjust_database_names(line.get('name')),
                    'oid': ident,
                    'identifier': ident,
                    'definition': definition,
                    'is_obsolete': is_obsolete,
                    'label': self.adjust_database_names(line.get('name')),
                    'url': self.add_miterm_url(ident)
                }
                processed_mi_list.append(dict_to_append)

        yield [processed_mi_list]
