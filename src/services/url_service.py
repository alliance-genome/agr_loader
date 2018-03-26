from loaders.transactions import Transaction

class UrlService(object):

    def get_complete_url(local_id, global_id, primary_id, crossRefMetaDataPk, graph):
        # Local and global are cross references, primary is the gene id.
        # TODO Update to dispatch?
        complete_url = None

        query = "match (crm:CrossReferenceMetaData) where crm.primaryKey = {parameter} return crm.page_url_prefix, crm.page_url_suffix"
        pk = crossRefMetaDataPk
        page_url_prefix = ""
        page_url_suffix = ""

        tx = Transaction(graph)
        returnSet = tx.run_single_parameter_query(query, pk)
        counter = 0
        for crm in returnSet:
            counter += 1
            page_url_prefix = crm['crm.page_url_prefix']
            page_url_suffix = crm['crm.page_url_suffix']
        if counter > 1:
            page_url_prefix = None
            print ("returning more than one gene: this is an error")
        complete_url = page_url_prefix + local_id + page_url_suffix

        if global_id.startswith('DRSC'):
            complete_url = None
        elif global_id.startswith('PANTHER'):
            panther_url = 'http://pantherdb.org/treeViewer/treeViewer.jsp?book=' + local_id + '&species=agr'
            split_primary = primary_id.split(':')[1]
            if primary_id.startswith('MGI'):
                complete_url = panther_url + '&seq=MGI=MGI=' + split_primary
            elif primary_id.startswith('RGD'):
                complete_url = panther_url + '&seq=RGD=' + split_primary
            elif primary_id.startswith('SGD'):
                complete_url = panther_url + '&seq=SGD=' + split_primary
            elif primary_id.startswith('FB'):
                complete_url = panther_url + '&seq=FlyBase=' + split_primary
            elif primary_id.startswith('WB'):
                complete_url = panther_url + '&seq=WormBase=' + split_primary
            elif primary_id.startswith('ZFIN'):
                complete_url = panther_url + '&seq=ZFIN=' + split_primary
            elif primary_id.startswith('HGNC'):
                complete_url = panther_url + '&seq=HGNC=' + split_primary

        return complete_url
