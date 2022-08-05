"""Unit tests.

Tests that methods return what they should etc.

Remember to remove bad_pages test once the olf code has been removed.
"""
from etl.helpers import ETLHelper


class TestClass():
    """Test Class."""

    etlh = ETLHelper()

    def test_get_shortname_from_taxon(self):
        """Test obtaining a species shortname from its taxon id."""
        lookups = {
            '10116': 'Rno',
            '8364': 'Xtr',
            '8355': 'Xla',
            'bad': None}  # Bad lookup returns None

        for key in lookups.keys():
            name = self.etlh.get_short_species_abbreviation(key)
            assert name == lookups[key]

    def test_get_mod_from_taxon(self):
        """Test obtaining a species mod from its taxon id."""
        lookups = {
            '10116': 'RGD',
            '8364': 'XB',
            '7227': 'FB',
            'bad': None}
        for key in lookups.keys():
            name = self.etlh.get_mod_from_taxon(key)
            assert name == lookups[key]

    def test_url_lookup_key_value(self):
        """Test url lookups."""
        # reset critical error
        self.etlh.rdh2.missing_keys = {}
        self.etlh.rdh2.missing_pages = {}
        self.etlh.rdh2.bad_pages = {}

        lookups = [{'key': 'RGD', 'value': '123456', 'page': None, 'result': 'https://rgd.mcw.edu/rgdweb/elasticResults.html?term=RGD:123456'},
                   {'key': 'RGD', 'value': '234567', 'page': 'allele', 'result': 'https://rgd.mcw.edu/rgdweb/report/gene/main.html?id=RGD:234567'},
                   {'key': 'FB', 'value': 'something', 'page': None, 'result': 'https://flybase.org/reports/something.html'},
                   {'key': 'Xenbase', 'value': 'something', 'page': None, 'result': 'https://www.xenbase.org/entry/something'},
                   {'key': 'FB', 'value': 'FBsomething', 'page': 'badpage', 'result': None},
                   {'key': 'BADKEY', 'value': 'something', 'page': None, 'result': None}]

        for item in lookups:
            url = self.etlh.rdh2.return_url_from_key_value(item['key'], item['value'], alt_page=item['page'])
            assert url == item['result']

        for item_name in self.etlh.rdh2.missing_keys.keys():
            assert 1 == self.etlh.rdh2.missing_keys[item_name]
        assert 'BADKEY-None' in self.etlh.rdh2.missing_keys.keys()
        assert 'BADKEY' in self.etlh.rdh2.missing_keys.keys()

        for item_name in self.etlh.rdh2.missing_pages.keys():
            assert 1 == self.etlh.rdh2.missing_pages[item_name]
            assert item_name == 'FB-badpage'

    def test_url_lookup(self):
        """Get url tests for ETLHelper."""
        self.etlh.rdh2.missing_keys = {}
        self.etlh.rdh2.missing_pages = {}
        self.etlh.rdh2.bad_pages = {}
        self.etlh.rdh2.bad_regex = {}

        lookups = [{'local_id': 'C5604', 'global_id': 'NCI:C5604',
                    'result': 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=C5604'},
                   {'local_id': 'badregexdoesnotmatch', 'global_id': 'MESH:badregexdoesnotmatch',
                    'result': 'https://www.ncbi.nlm.nih.gov/mesh/badregexdoesnotmatch'},
                   {'local_id': 'Cdiff', 'global_id': 'OMIM:1111', 'result': 'https://www.omim.org/entry/1111'}]

        for item in lookups:
            url = self.etlh.get_complete_url_ont(item['local_id'], item['global_id'])
            assert url == item['result']

        if self.etlh.rdh2.missing_keys.keys():
            assert 1 == "Should be no missing keys"
        if self.etlh.rdh2.missing_pages.keys():
            assert 1 == "Should be no missing pages"
        for item_name in self.etlh.rdh2.bad_pages.keys():
            # Due to local_id and global not matching we will get a bad pages.
            assert item_name == "OMIM-None"
        # mesh fails the regex so make sure we got an error message
        # we still get a url eror is logged.
        for item_name in self.etlh.rdh2.bad_regex.keys():
            assert 1 == self.etlh.rdh2.bad_regex[item_name]
            assert item_name == 'MESH'
