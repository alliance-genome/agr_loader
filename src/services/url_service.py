class UrlService(object):


    def get_page_complete_url(self, localId, xrefUrlMap, prefix, page):
        completeUrl = ""

        for rdstanza in xrefUrlMap:

            for resourceKey, valueMap in rdstanza.items():
                if resourceKey == prefix+page:

                    individualStanzaMap = rdstanza[prefix+page]

                    pageUrlPrefix = individualStanzaMap["page_url_prefix"]
                    pageUrlSuffix = individualStanzaMap["page_url_suffix"]

                    completeUrl = pageUrlPrefix + localId + pageUrlSuffix

        return completeUrl

    def get_no_page_complete_url(self, localId, xrefUrlMap, prefix, primaryId):

        completeUrl = ""
        globalId = prefix + localId
        for rdstanza in xrefUrlMap:
            for resourceKey, valueMap in rdstanza.items():
                if resourceKey == prefix:
                    individualStanzaMap = rdstanza[prefix]

                    defaultUrlPrefix = individualStanzaMap["default_url_prefix"]
                    defaultUrlSuffix= individualStanzaMap["default_url_suffix"]

                    completeUrl = defaultUrlPrefix+ localId + defaultUrlSuffix

                    if globalId.startswith('DRSC'):
                        completeUrl = None
                    elif globalId.startswith('PANTHER'):
                        panther_url = 'http://pantherdb.org/treeViewer/treeViewer.jsp?book=' + localId + '&species=agr'
                        split_primary = primaryId.split(':')[1]
                        if primaryId.startswith('MGI'):
                            completeUrl = panther_url + '&seq=MGI=MGI=' + split_primary
                        elif primaryId.startswith('RGD'):
                            completeUrl = panther_url + '&seq=RGD=' + split_primary
                        elif primaryId.startswith('SGD'):
                            completeUrl = panther_url + '&seq=SGD=' + split_primary
                        elif primaryId.startswith('FB'):
                            completeUrl = panther_url + '&seq=FlyBase=' + split_primary
                        elif primaryId.startswith('WB'):
                            completeUrl = panther_url + '&seq=WormBase=' + split_primary
                        elif primaryId.startswith('ZFIN'):
                            completeUrl = panther_url + '&seq=ZFIN=' + split_primary
                        elif primaryId.startswith('HGNC'):
                            completeUrl = panther_url + '&seq=HGNC=' + split_primary


        return completeUrl


