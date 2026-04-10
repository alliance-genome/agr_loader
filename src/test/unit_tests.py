"""Unit tests.

Tests that methods return what they should etc.

Remember to remove bad_pages test once the olf code has been removed.
"""
from etl.helpers import ETLHelper
from etl import OrthologyETL


class TestClass():
    """Test Class."""

    etlh = ETLHelper()

    def test_get_shortname_from_taxon(self):
        """Test obtaining a species shortname from its taxon id."""
        lookups = {
            '10116': 'Rno',
            '8364': 'Xtr',
            '8355': 'Xla',
            'bad': 'Alliance'}  # Bad lookup returns 'Alliance'

        for key in lookups.keys():
            name = self.etlh.get_short_species_abbreviation(key)
            assert name == lookups[key]

    def test_get_mod_from_taxon(self):
        """Test obtaining a species mod from its taxon id."""
        lookups = {
            '10116': 'RGD',
            '8364': 'XBXT',
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
                   {'local_id': 'Cdiff', 'global_id': 'MIM:1111', 'result': 'https://www.omim.org/MIM:1111'}] 
    
        for item in lookups:
            url = self.etlh.get_complete_url_ont(item['local_id'], item['global_id'])
            assert url == item['result']
    
        if self.etlh.rdh2.missing_keys.keys():
            assert 1 == "Should be no missing keys"
        if self.etlh.rdh2.missing_pages.keys():
            assert 1 == "Should be no missing pages"
        for item_name in self.etlh.rdh2.bad_pages.keys():
            # Due to local_id and global not matching we will get a bad pages.
            assert item_name == "MIM-None"  # Updated here
        # mesh fails the regex so make sure we got an error message
        # we still get a url error is logged.
        for item_name in self.etlh.rdh2.bad_regex.keys():
            assert 1 == self.etlh.rdh2.bad_regex[item_name]
            assert item_name == 'MESH'

    def test_orthology_excluded_gene_pairs(self):
        """Only the ticketed DIOPT VMA21/pdcd-2 pairs should be excluded."""
        blocked_pairs = [
            ("WB:WBGene00011116", "HGNC:22082"),
            ("WB:WBGene00011116", "MGI:1914298"),
            ("WB:WBGene00011116", "Xenbase:XB-GENE-5730849"),
            ("WB:WBGene00011116", "ZFIN:ZDB-GENE-081104-272"),
        ]

        for gene_1, gene_2 in blocked_pairs:
            assert OrthologyETL.is_excluded_gene_pair(gene_1, gene_2) is True
            assert OrthologyETL.is_excluded_gene_pair(gene_2, gene_1) is True

        assert OrthologyETL.is_excluded_gene_pair("WB:WBGene00011116", "RGD:1566155") is False
        assert OrthologyETL.is_excluded_gene_pair("WB:WBGene00011115", "HGNC:22082") is False

    def test_orthology_excluded_gene_pair_cleanup_query(self):
        """The cleanup query should remove stale copies of the excluded orthology pairs."""
        cleanup_query = OrthologyETL.get_excluded_gene_pair_cleanup_query()

        blocked_pairs = [
            ("WB:WBGene00011116", "HGNC:22082"),
            ("WB:WBGene00011116", "MGI:1914298"),
            ("WB:WBGene00011116", "Xenbase:XB-GENE-5730849"),
            ("WB:WBGene00011116", "ZFIN:ZDB-GENE-081104-272"),
        ]

        for gene_1, gene_2 in blocked_pairs:
            assert gene_1 in cleanup_query
            assert gene_2 in cleanup_query

        assert "DETACH DELETE join" in cleanup_query
        assert "DELETE orth" in cleanup_query
        assert "DELETE algo_rel" in cleanup_query
        assert "RGD:1566155" not in cleanup_query
