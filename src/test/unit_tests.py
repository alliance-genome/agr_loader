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
