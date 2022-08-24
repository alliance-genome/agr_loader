"""ETL Helper."""

import uuid
import logging
import json
import datetime
from .resource_descriptor_helper_2 import ResourceDescriptorHelper2
from .neo4j_helper import Neo4jHelper


class ETLHelper():
    """ETL Helper."""

    logger = logging.getLogger(__name__)
    rdh2 = ResourceDescriptorHelper2()
    rdh2.get_data()
    default_date_format = r'%Y-%m-%dT%H:%M:%SZ'

    @staticmethod
    def get_cypher_preferred_xref_text():
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
                         id.displayName = row.displayName,
                         id.preferred = row.preferred

                    MERGE (o)-[gcr:CROSS_REFERENCE]->(id) """

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
    def get_publication_object_cypher_text():
        """Get Cypher Publication Text."""
        return """
             MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pub.pubModId = row.pubModId,
                 pub.pubMedId = row.pubMedId,
                 pub.pubModUrl = row.pubModUrl,
                 pub.pubMedUrl = row.pubMedUrl
        """

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
            self.logger.critical("No reference page for %s", publication_mod_id)
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

    def get_complete_url_ont(self, local_id, global_id, key=None):
        """Get Complete 'ont'."""
        page = None
        if 'OMIM:PS' in global_id:
            page = 'ont'

        if not key:  # split not done before hand
            new_url = self.rdh2.return_url_from_identifier(global_id, page=page)
        else:
            new_url = self.rdh2.return_url_from_key_value(key, local_id, alt_page=page)

        return new_url

    def get_complete_pub_url(self, local_id, global_id, key=False):
        """Get Complete Pub URL.

        local_id: local value
        global_id: global_id may not be just the id part
        key: If passed we do not need to do the regular expression to get key
             most routines will have this already so just send that later on.

        """
        if 'get_complete_pub_url' not in self.rdh2.deprecated_mess:
            self.logger.info("get_complete_pub_url is Deprecated please use return_url_from_identifier")
            self.rdh2.deprecated_mess['get_complete_pub_url'] = 1
        else:
            self.rdh2.deprecated_mess['get_complete_pub_url'] += 1

        return self.rdh2.return_url_from_identifier(global_id)

    @staticmethod
    def process_identifiers(identifier):
        """Process Identifier."""
        if identifier.startswith("DRSC:"):
            # strip off DSRC prefix
            identifier = identifier[5:]
        return identifier

    # Only used by orthology ETL. 
    # Please don't use elsewhere unless you know what you are doing.
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
            2697049: '',  # No SARS-CoV-2 prefix
            8364: '', # No X. tropicalis prefix
            8355: '' # No X. laevis prefix
        }

        new_identifier = species_dict[taxon_id] + identifier

        return new_identifier

    def get_short_species_abbreviation(self, taxon_id):  # noqa  # will be okay after removing old method
        """Get short Species Abbreviation."""
        short_species_abbreviation = 'Alliance'
        try:
            short_species_abbreviation = self.rdh2.get_short_name_from_taxon(taxon_id)
        except KeyError:
            self.logger.critical("Problem looking up short species name for %s", taxon_id)

        return short_species_abbreviation

    @staticmethod
    def go_annot_prefix_lookup(dataprovider):
        """GO Annotation Prefix Lookup."""
        if dataprovider in ["MGI", "Human"]:
            return ""
        return dataprovider + ":"

    def get_mod_from_taxon(self, taxon_id):
        """Get MOD from Taxon."""
        return self.rdh2.get_mod_from_taxon(taxon_id)

    def get_subtype_from_taxon(self, taxon_id):
        """Get Subtype from Taxon."""
        return self.rdh2.get_subtype_from_taxon(taxon_id)

    def get_page_complete_url(self, local_id, xref_url_map, prefix, page):
        """Get Page Complete URL."""
        if 'get_page_complete_url' not in self.rdh2.deprecated_mess:
            self.logger.info("get_page_complete_url is Deprecated please use return_url_from_key_value")
            self.rdh2.deprecated_mess['get_page_complete_url'] = 1
        else:
            self.rdh2.deprecated_mess['get_page_complete_url'] += 1
        return self.rdh2.return_url_from_key_value(prefix, local_id, alt_page=page)

    def get_expression_images_url(self, local_id, cross_ref_id, prefix):
        """Get expression Images URL."""
        if 'get_expression_images_url' not in self.rdh2.deprecated_mess:
            self.logger.info("get_expression_images_url is Deprecated please use return_url_from_key_value")
            self.rdh2.deprecated_mess['get_expression_images_url'] = 1
        else:
            self.rdh2.deprecated_mess['get_expression_images_url'] += 1
        return self.rdh2.return_url_from_key_value(prefix, local_id, alt_page='gene/expression_images')

    def get_no_page_complete_url(self, local_id, prefix, primary_id):
        """Get No Page Complete URL.

        No idea why its called get no page complete url.
        """
        if prefix.startswith('DRSC'):
            return None
        elif prefix.startswith('PANTHER'):
            page, primary_id = primary_id.split(':')
            new_url = self.rdh2.return_url_from_key_value('PANTHER', primary_id, page)
            if new_url:
                new_url = new_url.replace('PAN_BOOK', local_id)
        else:
            new_url = self.rdh2.return_url_from_key_value(prefix, local_id)
        return new_url

    # wrapper scripts to enable shortened call.
    def return_url_from_key_value(self, alt_key, value, alt_page=None):
        """Forward to rdh2."""
        return self.rdh2.return_url_from_key_value(alt_key, value, alt_page=alt_page)

    def return_url_from_identifier(self, identifier, page=None):
        """Forward to rdh2."""
        return self.rdh2.return_url_from_identifier(identifier, page=page)

    @staticmethod
    def load_release_info(data, sub_type, logger):
        """Grab and store release info.

        If data is missing then release will be "NotSpecified" in the database
        and "date_produced" will be None, so BAD ones can be looked up that way.
        """
        metadata = {'release': "NotSpecified",
                    'dataSubType': sub_type.get_data_provider(),
                    'date_produced': None,
                    'dataType': sub_type.data_type}
        try:
            metadata['date_produced'] = ETLHelper.check_date_format(data['metaData']['dateProduced'])
            metadata['release'] = data['metaData']['release']
        except (KeyError, TypeError):
            pass

        fields = []
        for k in metadata:
            fields.append(k + ": " + json.dumps(metadata[k]))
        Neo4jHelper().run_single_query("CREATE (o:ModFileMetadata {" + ",".join(fields) + "})")

    @staticmethod
    def check_date_format(dateString, logger=None):
        """Convert to datetime object.

        Possibles for now:-
          2021-01-26 14:22:24             '%Y-%m-%d %H:%M:%S'
          2020-08-26                      '%Y-%m-%d'
          02/03/2021                      '%Y/%m/%d'
          Mon Feb 22 10:47:31 2021        '%a %b %d %H:%M:%S %Y'
          Sat Jan 23 03:02:06 CST 2021
          2021-01-12T12:04:02+00:00
        """
        dtFormat = (r'%Y-%m-%d',
                    r'%Y-%m-%d %H:%M:%S',
                    r'%d/%m/%Y',
                    r'%Y/%m/%d',
                    r'%a %b %d %H:%M:%S %Y',
                    r"%d:%m:%Y %H:%M")  # you can add extra formats needed

        if not dateString:
            return None
        dateString = dateString.replace(' CST', '')
        try:
            return datetime.datetime.fromisoformat(dateString).strftime(ETLHelper.default_date_format)
        except ValueError:
            pass
        while True:
            try:
                for i in dtFormat:
                    try:
                        return datetime.datetime.strptime(dateString, i).strftime(ETLHelper.default_date_format)
                    except ValueError:
                        pass
            except ValueError:
                pass
            if logger:
                logger.warning("Could not convert " + dateString)
                logger.warning('No valid date format found. Try again:')
            return None

    @staticmethod
    def load_release_info_from_args(logger=None, release='NotSpecified', provider=None, sub_type='GFF', date_produced=None):
        """Add releasde info form args."""
        metadata = {'release': release,
                    'mod': provider,
                    'type': sub_type,
                    'date_produced': date_produced}

        if date_produced and type(date_produced) != 'datetime.datetime':
            metadata['date_produced'] = ETLHelper.check_date_format(date_produced)
        elif type(date_produced) == 'datetime.datetime':
            metadata['date_produced'] = metadata['date_produced'].strftime(ETLHelper.default_date_format)

        fields = []
        for k in metadata:
            fields.append(k + ": " + json.dumps(metadata[k]))
        logger.warning(",".join(fields))
        Neo4jHelper().run_single_query("CREATE (o:ModFileMetadata {" + ",".join(fields) + "})")
