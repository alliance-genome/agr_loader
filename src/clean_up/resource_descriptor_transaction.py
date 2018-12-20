
import logging

logger = logging.getLogger(__name__)

class ResourceDescriptorTransaction(object):

    def resource_descriptor_tx(self, data):
        '''
        Loads the resource descriptor data into Neo4j.
        Maps come in like this:

        "resource": resource,
        "default_url": default_url,
        "gid_pattern": gid_pattern,
        "page_name": page_name,
        "page_url": page_url,
        "page_url_prefix": page_url_prefix,
        "page_url_suffix": page_url_suffix,
        "default_url_prefix": default_url_prefix,
        "default_url_suffix": default_url_suffix

        '''

        query = """

            UNWIND $data AS row

            MERGE (crm:CrossReferenceMetaData {primaryKey:row.primaryKey})
                SET crm.uuid = row.uuid
                SET crm.resource = row.resource
                SET crm.default_url = row.default_url
                SET crm.default_url_prefix = row.default_url_prefix
                SET crm.default_url_suffix = row.default_url_suffix
                SET crm.gid_pattern = row.gid_pattern
                SET crm.page_name = row.page_name
                SET crm.page_url = row.page_url
                SET crm.page_url_prefix = row.page_url_prefix
                SET crm.page_url_suffix = row.page_url_suffix
                SET crm.default_url_prefix = row.page_url_prefix
                SET crm.default_url_suffix = row.page_url_suffix

        """

        self.execute_transaction(query, data)

