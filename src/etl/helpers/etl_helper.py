"""ETL Helper.

NOTES: This can ve removed eventually just explaining why some thiongs havse changed.
       local_id and global_id removed as those that use the global_id should
       just have that bit in the url string.

i.e. previously we had
def get_complete_pub_url(local_id, global_id):
    if global_id.startswith('MGI:'):
        return 'http://www.informatics.jax.org/accession/' + global_id
    elif global_id.startswith('FB:'):
        return 'http://flybase.org/reports/{}.html' + local_id
now we have
def get_complete_pub_url(self, local_id, global_id, key=False):
    if not key: # split not done
       return self.rdh2.return_url_from_identifier(global_id)
    else:
       return self.rdh2.return_url_from_key_value(key, local_id)

as the url stored have the MGI: or RGD: etc in the url already if they are required.
"""
import uuid
import logging
from .resource_descriptor_helper_2 import ResourceDescriptorHelper2


class ETLHelper():
    """ETL Helper."""

    logger = logging.getLogger(__name__)
    rdh2 = ResourceDescriptorHelper2()
    rdh2.get_data()

    @staticmethod
    def get_cypher_xref_text():
        """Get Cypher XREF Text."""
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
        """Get Cypher XREF Tuned Text."""
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
        """Merge Crossref Relationships."""
        return """ MERGE (o)-[gcr:CROSS_REFERENCE]->(id)"""

    @staticmethod
    def get_cypher_xref_text_interactions():
        """Get Cypger XREF Text Interactions."""
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
        """Get Cypher XREF Text Annotation Level."""
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

    def get_expression_pub_annotation_xref(self, publication_mod_id):
        """Get Expression Pub Annotation XREF."""
        url = None
        try:
            url = self.rdh2.return_url_from_identifier(publication_mod_id)
        except KeyError:
            self.logger.critical("No reference page for {}".format(publication_mod_id))
        return url

    @staticmethod
    def get_xref_dict(local_id, prefix, cross_ref_type, page,
                      display_name, cross_ref_complete_url, primary_id):
        """Get XREF Dict."""
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

    def get_species_order(self, taxon_id):
        """Get Species Order."""
        order = None
        try:
            order = self.rdh2.get_order(taxon_id)
        except KeyError:
            self.logger.critical("Could not find order for taxon_id '{}'".format(taxon_id))
        return order

    def species_name_lookup(self, alt_key):
        """Lookup species name using some key.

        alt_key: can be things like Taxon_id, (i.e. NCBITaxon:9606 or 9606)
                 any case mod name (i.e. Rgd, RGD),
                 common names (i.e. rat, rno)
        """
        species_name = None
        try:
            species_name = self.rdh2.get_full_name_from_key(alt_key)
        except KeyError:
            self.logger.critical("Could not find species name for {}".format(alt_key))

        return species_name

    def species_lookup_by_taxonid(self, taxon_id):
        """Species Lookup by Taxon ID."""
        return self.species_name_lookup(taxon_id)

    def species_lookup_by_data_provider(self, provider):
        """Species Lookup by Data Provider."""
        return self.species_name_lookup(provider)

    def data_provider_lookup(self, species):
        """Lookup Data Provider."""
        mod = 'Alliance'
        if species == 'Homo sapiens':
            mod = 'RGD'
        else:
            try:
                mod = self.rdh2.get_key(species)
            except KeyError:
                self.logger.critical("Using default {} as {} not found".format(mod, species))
        return mod

    def get_complete_url_ont(self, local_id, global_id, key=None):
        """Get Complete 'ont'."""
        complete_url = None
        page = None
        if 'OMIM:PS' in global_id:
            page = 'ont'
        # Can delete from here to
            complete_url = 'https://www.omim.org/phenotypicSeries/' + local_id
        elif 'OMIM' in global_id:
            complete_url = 'https://www.omim.org/entry/' + local_id
        # Check ORDO does not seem like a real code 'ORPHA' maybe?
        elif 'ORDO' in global_id:
            complete_url = 'https://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=EN&Expert=' + local_id
        elif 'MESH' in global_id:
            ETLHelper.logger.debug("BOB:MESH l={} g={}".format(local_id, global_id))
            complete_url = 'https://www.ncbi.nlm.nih.gov/mesh/' + local_id
        elif 'EFO' in global_id:
            ETLHelper.logger.debug("BOB:EFO l={} g={}".format(local_id, global_id))
            complete_url = 'http://www.ebi.ac.uk/efo/EFO_' + local_id
        elif 'KEGG' in global_id:
            ETLHelper.logger.debug("BOB:KEGG l={} g={}".format(local_id, global_id))
            complete_url = 'http://www.genome.jp/dbget-bin/www_bget?map' + local_id
        elif 'NCI' in global_id:
            ETLHelper.logger.debug("BOB:NCI l={} g={}".format(local_id, global_id))
            complete_url = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp' + '?dictionary=NCI_Thesaurus&code=' + local_id
        # here, after testing

        if not key:  # split not done
            new_url = self.rdh2.return_url_from_identifier(global_id, page=page)
        else:
            new_url = self.rdh2.return_url_from_key_value(key, local_id, alt_page=page)
        if new_url != complete_url:
            bad_key = "{}-{}".format(global_id.split(':')[0], 'pub')
            if bad_key not in self.rdh2.bad_pages:
                self.logger.critical("get_complete_pub_ont old url '{}' != new url '{}'".format(complete_url, new_url))
                self.rdh2.bad_pages[bad_key] = 1
            else:
                self.rdh2.bad_pages[bad_key] += 1

        return new_url

    def get_complete_pub_url(self, local_id, global_id, key=False):
        """Get Complete Pub URL.

        local_id: local value
        global_id: global_id may not be just the id part
        key: If passed we do not need to do the regular expression to get key
             most routines will have this already so just send that later on.

        """
        complete_url = None
        if global_id.startswith('MGI:'):
            complete_url = 'http://www.informatics.jax.org/accession/' + global_id
        elif global_id.startswith('RGD:'):
            complete_url = 'http://rgd.mcw.edu/rgdweb/search/search.html?term=' + local_id
        elif global_id.startswith('SGD:'):
            complete_url = 'http://www.yeastgenome.org/reference/' + local_id
        elif global_id.startswith('FB:'):
            complete_url = 'https://flybase.org/reports/' + local_id + '.html'
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

        new_url = self.rdh2.return_url_from_identifier(global_id)
        if new_url != complete_url:
            # just report the first one to reduce verboseness and count the rest.
            # Not great but tempory until we reove old method.
            bad_key = "{}-{}".format(global_id.split(':')[0], 'pub')
            if bad_key not in self.rdh2.bad_pages:
                self.logger.critical("get_complete_pub_url old url '{}' != new url '{}'".format(complete_url, new_url))
                self.rdh2.bad_pages[bad_key] = 1
            else:
                self.rdh2.bad_pages[bad_key] += 1

        return complete_url

    @staticmethod
    def process_identifiers(identifier):
        """Process Identifier."""
        if identifier.startswith("DRSC:"):
            # strip off DSRC prefix
            identifier = identifier.split(":", 1)[1]
        return identifier

    @staticmethod
    def add_agr_prefix_by_species_taxon(identifier, taxon_id):
        """Add AGR prefix by Species Taxon."""
        species_dict = {
            7955: 'ZFIN:',
            6239: 'WB:',
            10090: '',  # No MGI prefix
            10116: '',  # No RGD prefix
            559292: 'SGD:',
            4932: 'SGD:',
            7227: 'FB:',
            9606: '',  # No HGNC prefix
            2697049: ''  # No SARS-CoV-2 prefix
        }

        new_identifier = species_dict[taxon_id] + identifier

        return new_identifier

    def get_short_species_abbreviation(self, taxon_id):
        """Get short Species Abbreviation."""
        short_species_abbreviation = 'Alliance'
        try:
            abbr = self.rdh2.get_short_name(taxon_id)
        except KeyError:
            self.logger.critical("Problem looking up short species name for {}".format(taxon_id))

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
        elif taxon_id == 'NCBITaxon:2697049':
            short_species_abbreviation = 'SARS-CoV-2'

        if abbr != short_species_abbreviation:
            self.logger.critical("{} != {}".format(abbr, short_species_abbreviation))
        return short_species_abbreviation

    @staticmethod
    def go_annot_prefix_lookup(dataprovider):
        """GO Annotation Prefix Lookup."""
        if dataprovider in ["MGI", "Human"]:
            return ""
        return dataprovider + ":"

    def get_mod_from_taxon(self, taxon_id):
        """Get MOD from Taxon"""
        new_mod = self.rdh2.get_key(taxon_id)
        taxon_mod_dict = {
            '7955': 'ZFIN',
            '6239': 'WB',
            '10090': 'MGI',
            '10116': 'RGD',
            '559292': 'SGD',
            '4932': 'SGD',
            '7227': 'FB',
            '9606': 'HUMAN',
            '2697049': 'SARS-CoV-2'}

        mod = taxon_mod_dict[taxon_id]
        if mod != new_mod:
            self.logger.critical("gmft: {} != {}".format(new_mod, mod))
        return mod

    def get_taxon_from_mod(self, mod):
        """Get Taxon From MOD."""
        new_tax = self.rdh2.get_taxon_from_key(mod)

        taxon_mod_dict = {
            'ZFIN': '7955',
            'WB': '6239',
            'MGI': '10090',
            'RGD': '10116',
            'SGD': '559292',
            'FB': '7227',
            'HUMAN': '9606',
            'SARS-CoV-2': '2697049'}

        # Attempt to get the taxon ID, return the MOD ID if the taxon is not found.
        tax = taxon_mod_dict.get(mod, mod)
        if new_tax != tax:
            self.logger.critical("gtfm: {} != {}".format(new_tax, tax))

    def get_page_complete_url(self, local_id, xref_url_map, prefix, page):
        """Get Page Complete URL."""

        complete_url = ""
        for rdstanza in xref_url_map:

            for resource_key in dict(rdstanza.items()):
                if resource_key == prefix + page:
                    individual_stanza_map = rdstanza[prefix + page]

                    page_url_prefix = individual_stanza_map["page_url_prefix"]
                    page_url_suffix = individual_stanza_map["page_url_suffix"]

                    complete_url = page_url_prefix + local_id + page_url_suffix

        new_url = self.rdh2.return_url_from_key_value(prefix, local_id, alt_page=page)
        if new_url != complete_url and (new_url is not None and complete_url != ''):
            mess = "BOB: prefix='{}' page='{}' local_id='{}': gpcu new '{}' != old '{}'".\
                format(prefix, page, local_id, new_url, complete_url)
            self.logger.critical(mess)
        return complete_url

    def get_expression_images_url(self, local_id, cross_ref_id, prefix):
        """Get expression Images URL."""
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
            url = "https://flybase.org/reports/" + local_id + ".html#expression"

        new_url = self.rdh2.return_url_from_key_value(prefix, local_id, alt_page='gene/expression_images')
        if new_url != url:
            self.logger.critical("old {} != new {}".format(url, new_url))
            self.logger.critical("{} {} {}".format(local_id, cross_ref_id, prefix))
        return url

    def get_no_page_complete_url(self, local_id, xref_url_map, prefix, primary_id):
        """Get No Page Complete URL.

        No idea why its called get no page complete url.
        """
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

        if global_id.startswith('DRSC'):
            return None
        elif global_id.startswith('PANTHER'):
            page, primary_id = split_primary = primary_id.split(':')
            new_url = self.rdh2.return_url_from_key_value('PANTHER', primary_id, page).replace('PAN_BOOK', local_id)

        else:
            new_url = self.rdh2.return_url_from_key_value(prefix, local_id)
        if complete_url != new_url:

            mess = "local = '{}', prefix= '{}', primary_id = '{}', url = {}, new_url = {}".\
                format(local_id, prefix, primary_id, complete_url, new_url)
            self.logger.critical(mess)
        return complete_url
