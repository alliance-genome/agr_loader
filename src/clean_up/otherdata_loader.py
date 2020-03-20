'''Other Data Loader'''

import logging

from .resource_descriptor import ResourceDescriptorService
from .resource_descriptor_transaction import ResourceDescriptorTransaction



class OtherDataLoader():
    '''Other Data Loader'''

    logger = logging.getLogger(__name__)

    def __init__(self):
        self.batch_size = 4000

    def run_loader(self):
        self.load_mol()
        self.load_additional_datasets()
        self.add_inferred_disease_annotations()
        #self.load_resource_descriptors()

    def load_resource_descriptors(self):
        self.logger.info("Extracting and loading resource descriptors")
        ResourceDescriptorTransaction().resource_descriptor_tx(ResourceDescriptorService().get_data())
