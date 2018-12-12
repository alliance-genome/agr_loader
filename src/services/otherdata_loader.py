import logging
import csv

from .mol_int_ext import MolIntExt
from .mol_int_transaction import MolIntTransaction
from .resource_descriptor import ResourceDescriptorService
from .resource_descriptor_transaction import ResourceDescriptorTransaction

logger = logging.getLogger(__name__)

class OtherDataLoader(object):
    def __init__(self):
        self.batch_size = 4000

    def run_loader(self):
        self.load_mol()
        self.load_additional_datasets()
        self.add_inferred_disease_annotations()
        #self.load_resource_descriptors()

    def load_mol(self):
        logger.info("Extracting and Loading Molecular Interaction data.")
        mol_int_data = MolIntExt().get_data(self.batch_size)

        first_entry, first_entry_xref = next(mol_int_data) # Used for obtaining the keys in the dictionary.

        fieldnames_keys = list(first_entry[0]) # Extract the fieldnames.
        fieldnames_xref = list(first_entry_xref[0])

        logger.info('Writing interactions to CSV.')
        with open('tmp/interactions.csv', mode='w') as int_csv, open('tmp/xref_interactions.csv', mode='w') as int_xref:
            interactions_writer = csv.DictWriter(int_csv, fieldnames=fieldnames_keys, quoting=csv.QUOTE_ALL)
            xref_int_writer = csv.DictWriter(int_xref, fieldnames=fieldnames_xref, quoting=csv.QUOTE_ALL)

            interactions_writer.writeheader() # Write the header.
            interactions_writer.writerows(first_entry) # Write the first entry from earlier.

            xref_int_writer.writeheader()
            xref_int_writer.writerows(first_entry_xref)

            for mol_int_list_of_entries, mol_int_xref_entries in mol_int_data:
                interactions_writer.writerows(mol_int_list_of_entries)
                xref_int_writer.writerows(mol_int_xref_entries)

        logger.info('Loading interactions into Neo4j via CSV.')
        MolIntTransaction().mol_int_tx()
        logger.info('Finished loading interactions into Neo4j via CSV.')

    def load_resource_descriptors(self):
        logger.info("Extracting and loading resource descriptors")
        ResourceDescriptorTransaction().resource_descriptor_tx(ResourceDescriptorService().get_data())
