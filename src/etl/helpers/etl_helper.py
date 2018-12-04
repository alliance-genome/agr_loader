import uuid

class ETLHelper(object):
    
    @staticmethod
    def get_cypher_xref_text():
        return """
                MERGE (id:CrossReference:Identifier {primaryKey:row.primaryKey})
                    ON CREATE SET id.name = row.id,
                     id.globalCrossRefId = row.globalCrossRefId,
                     id.localId = row.localId,
                     id.crossRefCompleteUrl = row.crossRefCompleteUrl,
                     id.prefix = row.prefix,
                     id.crossRefType = row.crossRefType,
                     id.uuid = row.uuid,
                     id.page = row.page,
                     id.primaryKey = row.primaryKey,
                     id.displayName = row.displayName

                MERGE (o)-[gcr:CROSS_REFERENCE]->(id) """

    @staticmethod
    def get_xref_dict(localId, prefix, crossRefType, page, displayName, crossRefCompleteUrl, primaryId):
        globalXrefId = prefix+":"+localId
        crossReference = {
            "id": globalXrefId,
            "globalCrossRefId": globalXrefId,
            "localId": localId,
            "crossRefCompleteUrl": crossRefCompleteUrl,
            "prefix": prefix,
            "crossRefType": crossRefType,
            "uuid":  str(uuid.uuid4()),
            "page": page,
            "primaryKey": primaryId,
            "displayName": displayName
        }
        return crossReference

    @staticmethod
    def species_lookup_by_taxonid(taxon_id):
        if taxon_id in "NCBITaxon:7955":
            return "Danio rerio"
        elif taxon_id in "NCBITaxon:6239":
            return "Caenorhabditis elegans"
        elif taxon_id in "NCBITaxon:10090":
            return "Mus musculus"
        elif taxon_id in "NCBITaxon:10116":
            return "Rattus norvegicus"
        elif taxon_id in "NCBITaxon:559292":
            return "Saccharomyces cerevisiae"
        elif taxon_id in "taxon:559292":
            return "Saccharomyces cerevisiae"
        elif taxon_id in "NCBITaxon:7227":
            return "Drosophila melanogaster"
        elif taxon_id in "NCBITaxon:9606":
            return "Homo sapiens"
        else:
            return None

    @staticmethod
    def species_lookup_by_data_provider(provider):
        if provider in "ZFIN":
            return "Danio rerio"
        elif provider in "MGI":
            return "Mus musculus"
        elif provider in "FB":
            return "Drosophila melanogaster"
        elif provider in "RGD":
            return "Rattus norvegicus"
        elif provider in "WB":
            return "Caenorhabditis elegans"
        elif provider in "SGD":
            return "Saccharomyces cerevisiae"
        elif provider in "Human":
            return "Homo sapiens"
        else:
            return None

    @staticmethod
    def data_provider_lookup(species):
        if species == 'Danio rerio':
            return 'ZFIN'
        elif species == 'Mus musculus':
            return 'MGI'
        elif species == 'Drosophila melanogaster':
            return 'FB'
        elif species == 'Homo sapiens':
            return 'RGD'
        elif species == 'Rattus norvegicus':
            return 'RGD'
        elif species == 'Caenorhabditis elegans':
            return 'WB'
        elif species == 'Saccharomyces cerevisiae':
            return 'SGD'
        else:
            return 'Alliance'
        
    #TODO: add these to resourceDescriptors.yaml and remove hardcoding.
    @staticmethod
    def get_complete_url_ont (local_id, global_id):

        complete_url = None

        if 'OMIM' in global_id:
            complete_url = 'https://www.omim.org/entry/' + local_id
        if 'OMIM:PS' in global_id:
            complete_url = 'https://www.omim.org/phenotypicSeries/' + local_id
        if 'ORDO' in global_id:
            complete_url = 'http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=EN&Expert=' +local_id
        if 'MESH' in global_id:
            complete_url = 'https://www.ncbi.nlm.nih.gov/mesh/' + local_id
        if 'EFO' in global_id:
            complete_url = 'http://www.ebi.ac.uk/efo/EFO_' + local_id
        if 'KEGG' in global_id:
            complete_url ='http://www.genome.jp/dbget-bin/www_bget?map' +local_id
        if 'NCI' in global_id:
            complete_url = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=' + local_id

        return complete_url
    
    @staticmethod
    def get_complete_pub_url(local_id, global_id):
        complete_url = None

        if 'MGI' in global_id:
            complete_url = 'http://www.informatics.jax.org/accession/' + global_id
        if 'RGD' in global_id:
            complete_url = 'http://rgd.mcw.edu/rgdweb/search/search.html?term=' + local_id
        if 'SGD' in global_id:
            complete_url = 'http://www.yeastgenome.org/reference/' + local_id
        if 'FB' in global_id:
            complete_url = 'http://flybase.org/reports/' + local_id + '.html'
        if 'ZFIN' in global_id:
            complete_url = 'http://zfin.org/' + local_id
        if 'WB:' in global_id:
            complete_url = 'http://www.wormbase.org/db/misc/paper?name=' + local_id
        if 'PMID:' in global_id:
            complete_url = 'https://www.ncbi.nlm.nih.gov/pubmed/' + local_id

        return complete_url
    
    @staticmethod
    def process_identifiers(identifier, dataProviders):
        for dataProvider in dataProviders:
            dataProviderDict = {
                'DRSC': 'DRSC:'
            }

            # Remove the prefix of the identifier based on the dataProvider.
            prefix = dataProviderDict[dataProvider]

            if identifier.startswith(prefix):
                return identifier[len(prefix):]

    @staticmethod
    def add_agr_prefix_by_species(identifier, taxon_id):
        speciesDict = {
            7955: 'ZFIN:',
            6239: 'WB:',
            10090: '',  # No MGI prefix
            10116: '',  # No RGD prefix
            559292: 'SGD:',
            4932: 'SGD:',
            7227: 'FB:',
            9606: ''  # No HGNC prefix
        }

        new_identifier = speciesDict[taxon_id] + identifier

        return new_identifier
    
    @staticmethod
    def go_annot_prefix_lookup(dataprovider):
        if dataprovider == "MGI" or dataprovider == "Human":
            return ""
        else:
            return dataprovider + ":"
    
    @staticmethod
    def get_MOD_from_taxon(taxon_id):
    
        taxon_mod_dict = {
            '7955' : 'ZFIN',
            '6239' : 'WB',
            '10090' : 'MGI',
            '10116' : 'RGD',
            '559292' : 'SGD',
            '7227' : 'FB',
            '9606' : 'Human'
        }
    
        return taxon_mod_dict[taxon_id]
        
    
    @staticmethod
    def get_page_complete_url(localId, xrefUrlMap, prefix, page):
        completeUrl = ""

        for rdstanza in xrefUrlMap:

            for resourceKey, valueMap in rdstanza.items():
                if resourceKey == prefix+page:

                    individualStanzaMap = rdstanza[prefix+page]

                    pageUrlPrefix = individualStanzaMap["page_url_prefix"]
                    pageUrlSuffix = individualStanzaMap["page_url_suffix"]

                    completeUrl = pageUrlPrefix + localId + pageUrlSuffix

        return completeUrl

    @staticmethod
    def get_no_page_complete_url(localId, xrefUrlMap, prefix, primaryId):

        completeUrl = ""
        globalId = prefix + localId
        for rdstanza in xrefUrlMap:
            for resourceKey, valueMap in rdstanza.items():
                if resourceKey == prefix:
                    individualStanzaMap = rdstanza[prefix]

                    defaultUrlPrefix = individualStanzaMap["default_url_prefix"]
                    defaultUrlSuffix = individualStanzaMap["default_url_suffix"]

                    completeUrl = defaultUrlPrefix + localId + defaultUrlSuffix

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
