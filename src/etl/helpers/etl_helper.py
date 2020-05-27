"""ETL Helper"""

import uuid
import logging


class ETLHelper():
    """ETL Helper"""

    logger = logging.getLogger(__name__)

    @staticmethod
    def get_cypher_xref_text():
        """Get Cypher XREF Text"""

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
    def get_cypher_xref_tuned_text():
        """Get Cypher XREF Tuned Text"""

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
                     id.displayName = row.displayName"""


    @staticmethod
    def merge_crossref_relationships():
        """Merge Crossref Relationships"""

        return """ MERGE (o)-[gcr:CROSS_REFERENCE]->(id)"""


    @staticmethod
    def get_cypher_xref_text_interactions():
        """Get Cypger XREF Text Interactions"""

        return """
                MERGE (id:CrossReference:Identifier {primaryKey:row.primaryKey, crossRefType:row.crossRefType})
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
    def get_cypher_xref_text_annotation_level():
        """Get Cypher XREF Text Annotation Level"""

        return """
                MERGE (id:CrossReference:Identifier {primaryKey:row.primaryKey})
                    SET id.name = row.id,
                     id.globalCrossRefId = row.globalCrossRefId,
                     id.localId = row.localId,
                     id.crossRefCompleteUrl = row.crossRefCompleteUrl,
                     id.prefix = row.prefix,
                     id.crossRefType = row.crossRefType,
                     id.uuid = row.uuid,
                     id.page = row.page,
                     id.primaryKey = row.primaryKey,
                     id.displayName = row.displayName,
                     id.curatedDB = apoc.convert.toBoolean(row.curatedDB),
                     id.loadedDB = apoc.convert.toBoolean(row.loadedDB)

                MERGE (o)-[gcr:ANNOTATION_SOURCE_CROSS_REFERENCE]->(id) """


    @staticmethod
    def get_expression_pub_annotation_xref(publication_mod_id):
        """Get Expression Pub Annotation XREF"""

        if publication_mod_id is not None:
            pub_mod_local_id = publication_mod_id.split(":")[1]
            if "MGI:" in publication_mod_id:
                pub_mod_url = "http://www.informatics.jax.org/reference/" + publication_mod_id
            elif "ZFIN:" in publication_mod_id:
                pub_mod_url = "http://zfin.org/" + publication_mod_id
            elif "SGD:" in publication_mod_id:
                pub_mod_url = "https://www.yeastgenome.org/reference/" + pub_mod_local_id
            elif "WB:" in publication_mod_id:
                pub_mod_url = "https://www.wormbase.org/db/get?name=" + pub_mod_local_id + ";class=Paper"
            elif "RGD:" in publication_mod_id:
                pub_mod_url = "https://rgd.mcw.edu" + "/rgdweb/report/reference/main.html?id=" + pub_mod_local_id
            elif "FB:" in publication_mod_id:
                pub_mod_url = "http://flybase.org/reports/" + pub_mod_local_id

        return pub_mod_url


    @staticmethod
    def get_xref_dict(local_id, prefix, cross_ref_type, page,
                      display_name, cross_ref_complete_url, primary_id):
        """Get XREF Dict"""

        global_xref_id = prefix + ":" + local_id
        cross_reference = {
            "id": global_xref_id,
            "globalCrossRefId": global_xref_id,
            "localId": local_id,
            "crossRefCompleteUrl": cross_ref_complete_url,
            "prefix": prefix,
            "crossRefType": cross_ref_type,
            "uuid":  str(uuid.uuid4()),
            "page": page,
            "primaryKey": primary_id,
            "displayName": display_name}

        return cross_reference


    @staticmethod
    def get_species_order(taxon_id):
        """Get Species Order"""

        order = None
        if taxon_id in "NCBITaxon:7955":
            order = 40
        elif taxon_id in "NCBITaxon:6239":
            order = 60
        elif taxon_id in "NCBITaxon:10090":
            order = 30
        elif taxon_id in "NCBITaxon:10116":
            order = 20
        elif taxon_id in "NCBITaxon:4932":
            order = 70
        elif taxon_id in "NCBITaxon:559292":
            order = 70
        elif taxon_id in "NCBITaxon:7227":
            order = 50
        elif taxon_id in "NCBITaxon:9606":
            order = 10

        return order


    @staticmethod
    def species_lookup_by_taxonid(taxon_id):
        """Species Lookup by Taxon ID"""

        species_name = None
        if taxon_id in "NCBITaxon:7955":
            species_name = "Danio rerio"
        elif taxon_id in "NCBITaxon:6239":
            species_name = "Caenorhabditis elegans"
        elif taxon_id in "NCBITaxon:10090":
            species_name = "Mus musculus"
        elif taxon_id in "NCBITaxon:10116":
            species_name = "Rattus norvegicus"
        elif taxon_id in "NCBITaxon:559292":
            species_name = "Saccharomyces cerevisiae"
        elif taxon_id in "taxon:559292":
            species_name = "Saccharomyces cerevisiae"
        elif taxon_id in "NCBITaxon:7227":
            species_name = "Drosophila melanogaster"
        elif taxon_id in "NCBITaxon:9606":
            species_name = "Homo sapiens"

        return species_name


    @staticmethod
    def species_lookup_by_data_provider(provider):
        """Species Lookup by Data Provider"""

        species_name = None
        if provider == "ZFIN":
            species_name = "Danio rerio"
        elif provider == "MGI":
            species_name = "Mus musculus"
        elif provider == "FB":
            species_name = "Drosophila melanogaster"
        elif provider == "RGD":
            species_name = "Rattus norvegicus"
        elif provider == "WB":
            species_name = "Caenorhabditis elegans"
        elif provider == "SGD":
            species_name = "Saccharomyces cerevisiae"
        elif provider == "Human":
            species_name = "Homo sapiens"
        elif provider == "HUMAN":
            species_name = "Homo sapiens"

        return species_name


    @staticmethod
    def data_provider_lookup(species):
        """Data Provider Lookup"""

        mod = 'Alliance'
        if species == 'Danio rerio':
            mod = 'ZFIN'
        elif species == 'Mus musculus':
            mod = 'MGI'
        elif species == 'Drosophila melanogaster':
            mod = 'FB'
        elif species == 'Homo sapiens':
            mod = 'RGD'
        elif species == 'Rattus norvegicus':
            mod = 'RGD'
        elif species == 'Caenorhabditis elegans':
            mod = 'WB'
        elif species == 'Saccharomyces cerevisiae':
            mod = 'SGD'

        return mod


    #TODO: add these to resourceDescriptors.yaml and remove hardcoding.
    @staticmethod
    def get_complete_url_ont(local_id, global_id):
        """Get Complete URL"""

        complete_url = None
        if 'OMIM:PS' in global_id:
            complete_url = 'https://www.omim.org/phenotypicSeries/' + local_id
        elif 'OMIM' in global_id:
            complete_url = 'https://www.omim.org/entry/' + local_id
        elif 'ORDO' in global_id:
            complete_url = 'http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=EN&Expert=' +local_id
        elif 'MESH' in global_id:
            complete_url = 'https://www.ncbi.nlm.nih.gov/mesh/' + local_id
        elif 'EFO' in global_id:
            complete_url = 'http://www.ebi.ac.uk/efo/EFO_' + local_id
        elif 'KEGG' in global_id:
            complete_url = 'http://www.genome.jp/dbget-bin/www_bget?map' + local_id
        elif 'NCI' in global_id:
            complete_url = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp'\
                            + '?dictionary=NCI_Thesaurus&code=' + local_id

        return complete_url


    @staticmethod
    def get_complete_pub_url(local_id, global_id):
        """Get Complete Pub URL"""

        complete_url = None
        if global_id.startswith('MGI:'):
            complete_url = 'http://www.informatics.jax.org/accession/' + global_id
        elif global_id.startswith('RGD:'):
            complete_url = 'http://rgd.mcw.edu/rgdweb/search/search.html?term=' + local_id
        elif global_id.startswith('SGD:'):
            complete_url = 'http://www.yeastgenome.org/reference/' + local_id
        elif global_id.startswith('FB:'):
            complete_url = 'http://flybase.org/reports/' + local_id + '.html'
        elif global_id.startswith('ZFIN:'):
            complete_url = 'http://zfin.org/' + local_id
        elif global_id.startswith('WB:'):
            complete_url = 'http://www.wormbase.org/db/misc/paper?name=' + local_id
        elif global_id.startswith('PMID:'):
            complete_url = 'https://www.ncbi.nlm.nih.gov/pubmed/' + local_id
        elif global_id.startswith('OMIM:'):
            complete_url = 'https://www.omim.org/entry/' + local_id
        elif global_id.startswith('ORPHA:'):
            complete_url = 'https://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=EN&Expert=' + local_id
        return complete_url


    @staticmethod
    def process_identifiers(identifier):
        """Process Identifier"""

        if identifier.startswith("DRSC:"):
            # strip off DSRC prefix
            identifier = identifier.split(":", 1)[1]
        return identifier


    @staticmethod
    def add_agr_prefix_by_species_taxon(identifier, taxon_id):
        """Add AGR prefix by Species Taxon"""

        species_dict = {
            7955: 'ZFIN:',
            6239: 'WB:',
            10090: '',  # No MGI prefix
            10116: '',  # No RGD prefix
            559292: 'SGD:',
            4932: 'SGD:',
            7227: 'FB:',
            9606: ''  # No HGNC prefix
        }

        new_identifier = species_dict[taxon_id] + identifier

        return new_identifier


    @staticmethod
    def get_short_species_abbreviation(taxon_id):
        """Get short Species Abbreviation"""

        short_species_abbreviation = 'Alliance'
        if taxon_id == 'NCBITaxon:7955':
            short_species_abbreviation = 'Dre'
        elif taxon_id == 'NCBITaxon:7227':
            short_species_abbreviation = 'Dme'
        elif taxon_id == 'NCBITaxon:10090':
            short_species_abbreviation = 'Mmu'
        elif taxon_id == 'NCBITaxon:6239':
            short_species_abbreviation = 'Cel'
        elif taxon_id == 'NCBITaxon:10116':
            short_species_abbreviation = 'Rno'
        elif taxon_id == 'NCBITaxon:559292':
            short_species_abbreviation = 'Sce'
        elif taxon_id == 'NCBITaxon:9606':
            short_species_abbreviation = 'Hsa'

        return short_species_abbreviation


    @staticmethod
    def go_annot_prefix_lookup(dataprovider):
        """GO Annotation Prefix Lookup"""

        if dataprovider in ["MGI", "Human"]:
            return ""
        return dataprovider + ":"


    @staticmethod
    def get_mod_from_taxon(taxon_id):
        """Get MOD from Taxon"""

        taxon_mod_dict = {
            '7955': 'ZFIN',
            '6239': 'WB',
            '10090': 'MGI',
            '10116': 'RGD',
            '559292': 'SGD',
            '4932': 'SGD',
            '7227': 'FB',
            '9606': 'HUMAN'}

        return taxon_mod_dict[taxon_id]


    @staticmethod
    def get_taxon_from_mod(mod):
        """Get Taxon From MOD"""

        taxon_mod_dict = {
            'ZFIN': '7955',
            'WB': '6239',
            'MGI': '10090',
            'RGD': '10116',
            'SGD': '559292',
            'FB': '7227',
            'HUMAN': '9606'}

        # Attempt to get the taxon ID, return the MOD ID if the taxon is not found.
        return taxon_mod_dict.get(mod, mod)


    @staticmethod
    def get_page_complete_url(local_id, xref_url_map, prefix, page):
        """Get Patge Complet URL"""

        complete_url = ""
        for rdstanza in xref_url_map:

            for resource_key in dict(rdstanza.items()):
                if resource_key == prefix + page:
                    individual_stanza_map = rdstanza[prefix + page]

                    page_url_prefix = individual_stanza_map["page_url_prefix"]
                    page_url_suffix = individual_stanza_map["page_url_suffix"]

                    complete_url = page_url_prefix + local_id + page_url_suffix

        return complete_url


    @staticmethod
    def get_expression_images_url(local_id, cross_ref_id):
        """Get expression Images URL"""

        url = ""
        if 'MGI' in cross_ref_id:
            url = "http://www.informatics.jax.org/gxd/marker/MGI:" + local_id \
                                                                   + "?tab=imagestab"
        elif 'ZFIN' in cross_ref_id:
            url = "https://zfin.org/" + local_id + "/wt-expression/images"
        elif 'WB' in cross_ref_id:
            url = "https://www.wormbase.org/db/get?name=" + local_id \
                                                          + ";class=Gene;widget=expression"
        elif 'FB' in cross_ref_id:
            url = "http://flybase.org/reports/" + local_id + ".html#expression"

        return url


    @staticmethod
    def get_no_page_complete_url(local_id, xref_url_map, prefix, primary_id):
        """Get No Page Complete URL"""


        complete_url = ""
        global_id = prefix + local_id
        for rdstanza in xref_url_map:
            for resource_key in dict(rdstanza.items()):
                if resource_key == prefix:
                    individual_stanza_map = rdstanza[prefix]

                    default_url_prefix = individual_stanza_map["default_url_prefix"]
                    default_url_suffix = individual_stanza_map["default_url_suffix"]

                    complete_url = default_url_prefix + local_id + default_url_suffix

                    if global_id.startswith('DRSC'):
                        complete_url = None
                    elif global_id.startswith('PANTHER'):
                        panther_url = 'http://pantherdb.org/treeViewer/treeViewer.jsp?book='\
                                          + local_id + '&species=agr'
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
