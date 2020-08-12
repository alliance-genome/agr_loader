"""Unit tests.

Tests that methods return what they should etc.
"""
from etl.helpers import ResourceDescriptorHelper2, ETLHelper


class TestClass():
    """Test Class."""

    etlh = ETLHelper()

    def test_get_species_name_from_various_keys(self):
        """Test getting valid species names from DB."""
        lookups = {'RGD': 'Rattus norvegicus',
                   'NCBITaxon:10116': 'Rattus norvegicus',
                   '10116': 'Rattus norvegicus',
                   'Cel': 'Caenorhabditis elegans',
                   'worm': 'Caenorhabditis elegans',
                   'Dme': 'Drosophila melanogaster',
                   'bad': None}  # Bad lookup returns None

        for key in lookups.keys():
            name = self.etlh.species_name_lookup(key)
            assert name == lookups[key]

    def test_get_species_order(self):
        """Test getting order."""
        lookups = {'RGD': 20,
                   'NCBITaxon:10116': 20,
                   '10116': 20,
                   'Cel': 60,
                   'worm': 60,
                   'Dme': 50,
                   'bad': None}  # Bad lookup returns None

        for key in lookups.keys():
            name = self.etlh.get_species_order(key)
            assert name == lookups[key]

    def test_data_provider_lookup(self):
        """Test provider lookup."""
        lookups = {'RGD': 'RGD',
                   'NCBITaxon:10116': 'RGD',
                   'worm': 'WB',
                   'Dme': 'FB',
                   'Saccharomyces cerevisiae': 'SGD',
                   'Homo sapiens': 'RGD',  # Wierd one
                   'bad': None}  # Bad lookup returns None

        for key in lookups.keys():
            name = self.etlh.data_provider_lookup(key)
            assert name == lookups[key]

    def test_url_lookup_key_value(self):
        """Test url lookups."""

        lookups = [{'key': 'RGD', 'value': '123456', 'page': None, 'result': 'https://rgd.mcw.edu/rgdweb/elasticResults.html?term=RGD:123456'},
                   {'key': 'RGD', 'value': '234567', 'page': 'allele', 'result': 'https://rgd.mcw.edu/rgdweb/report/gene/main.html?id=RGD:234567'},
                   {'key': 'FB', 'value': 'something', 'page': None, 'result': 'https://flybase.org/reports/something.html'},
                   {'key': 'FB', 'value': 'FBsomething', 'page': 'badpage', 'result': None},
                   {'key': 'BADKEY', 'value': 'something', 'page': None, 'result': None}]

        for item in lookups:
            url = self.etlh.rdh2.return_url_from_key_value(item['key'], item['value'], alt_page=item['page'])
            assert url == item['result']

       url = self.etlh.get_complete_pub_ont('C5604', 'NCI:C5604')
       