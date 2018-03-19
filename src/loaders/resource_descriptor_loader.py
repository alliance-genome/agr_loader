from .transactions.resource_descriptor import ResourceDescriptorTransaction

class ResourceDescriptorLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_resource_descriptor(self, data):
        tx = ResourceDescriptorTransaction(self.graph)
        tx.resource_descriptor_tx(data)
        