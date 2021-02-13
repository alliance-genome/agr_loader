"""Molecular Interaction ETL."""

import logging
import uuid
import csv
import re
import sys
import itertools

from etl import ETL
from transactors import CSVTransactor, Neo4jTransactor
from etl.helpers import Neo4jHelper, ETLHelper

# Test loading this requires:
# BGI: [FB, SGD, WB, ZFIN, RGD, MGI, HUMAN, SARS-CoV-2]
# ONTOLOGY: [MI]
# INTERACTION-MOL: [COMBINED]



class MolecularInteractionETL(ETL):
    """Molecular Interaction ETL."""

    logger = logging.getLogger(__name__)

    # Query templates which take params and will be processed later

    main_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
        MATCH (g1:Gene {primaryKey:row.interactor_A})
        MATCH (g2:Gene {primaryKey:row.interactor_B})

        MATCH (mi:MITerm) WHERE mi.primaryKey = row.detection_method
        MATCH (sdb:MITerm) WHERE sdb.primaryKey = row.source_database
        MATCH (adb:MITerm) WHERE adb.primaryKey = row.aggregation_database
        MATCH (ita:MITerm) WHERE ita.primaryKey = row.interactor_A_type
        MATCH (itb:MITerm) WHERE itb.primaryKey = row.interactor_B_type
        MATCH (ira:MITerm) WHERE ira.primaryKey = row.interactor_A_role
        MATCH (irb:MITerm) WHERE irb.primaryKey = row.interactor_B_role
        MATCH (it:MITerm) WHERE it.primaryKey = row.interaction_type

        //Create the relationship between the two genes.
        CREATE (g1)-[iw:INTERACTS_WITH {uuid:row.uuid}]->(g2)

        //Create the Association node to be used for the object.
        CREATE (oa:Association {primaryKey:row.uuid})
            SET oa :InteractionGeneJoin
            SET oa :InteractionMolecularGeneJoin
            SET oa.joinType = 'molecular_interaction'
        CREATE (g1)-[a1:ASSOCIATION]->(oa)
        CREATE (oa)-[a2:ASSOCIATION]->(g2)

        //Create the publication nodes and link them to the Association node.
        MERGE (pn:Publication {primaryKey:row.pub_med_id})
            ON CREATE SET pn.pubMedUrl = row.pub_med_url,
            pn.pubMedId = row.pub_med_id
        CREATE (oa)-[ev:EVIDENCE]->(pn)

        //Link detection method to the MI ontology.
        CREATE (oa)-[dm:DETECTION_METHOD]->(mi)

        //Link source database to the MI ontology.
        CREATE (oa)-[sd:SOURCE_DATABASE]->(sdb)

        //Link aggregation database to the MI ontology.
        CREATE (oa)-[ad:AGGREGATION_DATABASE]->(adb)

        //Link interactor roles and types to the MI ontology.
        CREATE (oa)-[ita1:INTERACTOR_A_TYPE]->(ita)
        CREATE (oa)-[itb1:INTERACTOR_B_TYPE]->(itb)
        CREATE (oa)-[ira1:INTERACTOR_A_ROLE]->(ira)
        CREATE (oa)-[irb1:INTERACTOR_B_ROLE]->(irb)

        //Link interaction type to the MI ontology.
        CREATE (oa)-[it1:INTERACTION_TYPE]->(it)
    """

    xref_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        // This needs to be a MERGE below.
        MATCH (o:InteractionGeneJoin :Association) WHERE o.primaryKey = row.reference_uuid
        """ + ETLHelper.get_cypher_xref_text()

    mod_xref_query_template = """

        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

            MATCH (o:Gene {primaryKey:row.dataId}) """ + ETLHelper.get_cypher_xref_text()

    def __init__(self, config):
        """Initiaslise object."""
        super().__init__()
        self.data_type_config = config

        # Initialize an instance of ResourceDescriptor for processing external links.
        # self.resource_descriptor_dict = ResourceDescriptorHelper2()
        self.missed_database_linkouts = set()
        self.successful_database_linkouts = set()
        self.ignored_database_linkouts = set()
        self.successful_mod_interaction_xrefs = []

    def _load_and_process_data(self):

        filepath = self.data_type_config.get_single_filepath()

        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        generators = self.get_generators(filepath, batch_size)

        query_template_list = [
            [self.main_query_template, commit_size, "mol_int_data.csv"],
            [self.xref_query_template, commit_size, "mol_int_xref.csv"],
            [self.mod_xref_query_template, commit_size, "mol_int_mod_xref.csv"]
        ]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

    @staticmethod
    def populate_genes():
        """Populate Genes."""
        master_gene_set = set()

        query = "MATCH (g:Gene) RETURN g.primaryKey"

        result = Neo4jHelper().run_single_query(query)

        for record in result:
            master_gene_set.add(record['g.primaryKey'])

        return master_gene_set

    @staticmethod
    def query_crossreferences(crossref_prefix):
        """Query Cross References."""
        query = """MATCH (g:Gene)-[C:CROSS_REFERENCE]-(cr:CrossReference)
                   WHERE cr.prefix = {parameter}
                   RETURN g.primaryKey, cr.globalCrossRefId"""
        return Neo4jHelper().run_single_parameter_query(query, crossref_prefix)

    def populate_crossreference_dictionary(self):
        """Populate the crossreference dictionary.

        We're populating a rather large dictionary to use for looking up Alliance genes by
        their crossreferences.
        Edit the list below if you'd like to add more crossreferences to the dictionary.
        The key of the dictionary is the crossreference and the value is the Alliance
        gene to which it resolves.
        """
        master_crossreference_dictionary = dict()

        # If additional crossreferences need to be used to find interactors, they can be added here.
        # Use the crossreference prefix as the dictionary name.
        # Also add a regex entry to the resolve_identifier function.
        master_crossreference_dictionary['UniProtKB'] = dict()
        master_crossreference_dictionary['ENSEMBL'] = dict()
        master_crossreference_dictionary['NCBI_Gene'] = dict()
        master_crossreference_dictionary['RefSeq'] = dict()

        for key in master_crossreference_dictionary:
            self.logger.info('Querying for %s cross references.', key)
            result = self.query_crossreferences(key)
            for record in result:
                cross_ref_record = None
                # Modify the cross reference ID to match the PSI MITAB format if necessary.
                # So far, this is just converting 'NCBI_Gene' to 'entrez gene/locuslink'.
                if record['cr.globalCrossRefId'].startswith('NCBI_Gene'):
                    cross_ref_record_split = record['cr.globalCrossRefId'].split(':')[1]
                    cross_ref_record = 'entrez gene/locuslink:' + cross_ref_record_split
                else:
                    cross_ref_record = record['cr.globalCrossRefId']

                # The crossreference dictionary is a list of genes
                # linked to a single crossreference.
                # Append the gene if the crossref dict entry exists.
                # Otherwise, create a list and append the entry.
                if cross_ref_record.lower() in master_crossreference_dictionary[key]:
                    master_crossreference_dictionary[key][cross_ref_record.lower()].append(record['g.primaryKey'])
                else:
                    master_crossreference_dictionary[key][cross_ref_record.lower()] = []
                    master_crossreference_dictionary[key][cross_ref_record.lower()].append(record['g.primaryKey'])

                # The ids in PSI-MITAB files are lower case, hence the .lower() used above.

        return master_crossreference_dictionary

    def process_interaction_identifier(self, entry, additional_row):
        """Create cross references for all the external identifiers."""
        xref_main_list = []
        entries = None

        # Identifier types on this list DO NOT receive a
        # cross_ref_complete_url field for external linking.
        ignored_identifier_database_list = [
            # The following entries are not currently required.
            'brenda',
            'bmrb',
            'cell ontology',
            'chebi',
            'chembl compound',
            'efo',
            'flannotator',
            'intenz',
            'interpro',
            'mpidb',
            'omim',
            'pdbj',
            'pmc',
            'pride',
            'prints',
            'proteomexchange',
            'psi-mi',
            'pubmed',
            'go',
            'reactome',
            'refseq',
            'tissue list',
            'uniprotkb'
        ]

        if '|' in entry:
            entries = entry.split('|')
        else:
            entries = [entry]

        for individual in entries:
            """These links are for the individual interaction identifiers and link to the respective database."""
            xref_dict = {}
            page = 'gene/interactions'

            individual_prefix, individual_body, _ = self.etlh.rdh2.split_identifier(individual)
            # Capitalize the prefix to match the YAML
            # and change the prefix if necessary to match the YAML.

            xref_dict['localId'] = individual_body
            xref_dict['uuid'] = str(uuid.uuid4())
            xref_dict['id'] = individual
            xref_dict['displayName'] = individual_body
            xref_dict['primaryKey'] = individual
            xref_dict['crossRefType'] = 'interaction'
            xref_dict['page'] = page
            xref_dict['reference_uuid'] = None  # For association interactions (later).

            # Special case for dealing with FlyBase.
            # The identifier link needs to use row 25 from the psi-mitab file.
            # TODO Regex to check for FBig in additional_row?
            if individual.startswith('flybase:FBrf'):

                # primaryKey needs to be individual_body as opposed to individual.
                xref_dict['primaryKey'] = individual_body

                if '|' in additional_row:
                    individual = additional_row.split('|')[0]
                else:
                    individual = additional_row

                individual_prefix, individual_body, _ = self.etlh.rdh2.split_identifier(individual)

                regex_check = re.match('^flybase:FBig\\d{10}$', individual)
                if regex_check is None:
                    self.logger.critical(
                        """Fatal Error: During special handling of FlyBase molecular interaction
                           links, an FBig ID was not found.""")
                    self.logger.critical('Failed identifier: %s', individual)
                    self.logger.critical('PSI-MITAB row entry: %s', additional_row)
                    sys.exit(-1)

                # Change our prefix.
                individual_prefix = 'FB'

            # Special case for dealing with WormBase.
            if individual_prefix.startswith('wormbase'):
                individual_prefix = 'WB'

            # TODO Optimize and re-add this error tracking.
            if not individual.startswith(tuple(ignored_identifier_database_list)):
                try:
                    individual_url = self.etlh.rdh2.return_url_from_key_value(individual_prefix, individual_body, page)
                    xref_dict['crossRefCompleteUrl'] = individual_url
                except KeyError:
                    pass

            xref_dict['prefix'] = individual_prefix
            xref_dict['globalCrossRefId'] = individual

            xref_main_list.append(xref_dict)

        return xref_main_list

    def add_mod_interaction_links(self, gene_id):
        """Create an XREF linking back to interaction pages at each MOD for a particular gene.
        These links appear at the top of the molecular interactions table once per gene page.
        """
        xref_dict = {}
        page = 'gene/MODinteractions_molecular'

        individual_prefix, individual_body, _ = self.etlh.rdh2.split_identifier(gene_id)
        individual_url = self.etlh.rdh2.return_url_from_identifier(gene_id, page)

        # Exception for MGI
        if individual_prefix == 'MGI':
            xref_dict['displayName'] = gene_id
            xref_dict['id'] = gene_id
            xref_dict['globalCrossRefId'] = gene_id
            xref_dict['primaryKey'] = gene_id + page
        else:
            xref_dict['displayName'] = individual_body
            xref_dict['id'] = individual_body
            xref_dict['globalCrossRefId'] = individual_body
            xref_dict['primaryKey'] = individual_body + page

        xref_dict['prefix'] = individual_prefix
        xref_dict['localId'] = individual_body
        xref_dict['crossRefCompleteUrl'] = individual_url
        xref_dict['uuid'] = str(uuid.uuid4())
        xref_dict['crossRefType'] = page
        xref_dict['page'] = page
        xref_dict['reference_uuid'] = str(uuid.uuid4())

#       For matching to the gene when creating the xref relationship in Neo.
        xref_dict['dataId'] = gene_id
        # Add the gene_id of the identifier to a global list so we don't create unnecessary xrefs.
        self.successful_mod_interaction_xrefs.append(gene_id)

        return xref_dict

    def resolve_identifiers_by_row(self, row, master_gene_set, master_crossreference_dictionary):
        """Resolve Identifiers by Row."""
        interactor_a_rows = [0, 2, 4, 22]
        interactor_b_rows = [1, 3, 5, 23]

        interactor_a_resolved = None
        interactor_b_resolved = None

        for row_entry in interactor_a_rows:
            try:
                # We need to change uniprot/swiss-prot to uniprotkb for interactor a and b.
                # This is the only current prefix adjustment.
                # If we need to do more, we should break this out into a function or small piece of code.
                interactor_a = row[row_entry].replace("uniprot/swiss-prot:", "uniprotkb:")
                interactor_a_resolved = self.resolve_identifier(interactor_a,
                                                                master_gene_set,
                                                                master_crossreference_dictionary)
                if interactor_a_resolved is not None:
                    break
            except IndexError:  # Biogrid has less rows than other files, continue on IndexErrors.
                continue

        for row_entry in interactor_b_rows:
            try:
                interactor_b = row[row_entry].replace("uniprot/swiss-prot:", "uniprotkb:")
                interactor_b_resolved = self.resolve_identifier(interactor_b,
                                                                master_gene_set,
                                                                master_crossreference_dictionary)
                if interactor_b_resolved is not None:
                    break
            except IndexError:  # Biogrid has less rows than other files, continue on IndexErrors.
                continue

        return interactor_a_resolved, interactor_b_resolved

    def resolve_identifier(self, row_entry, master_gene_set, master_crossreference_dictionary):  # noqa
        """Resolve Identifier."""
        list_of_crossref_regex_to_search = [
            'uniprotkb:[\\w\\d_-]*$',
            'ensembl:[\\w\\d_-]*$',
            'entrez gene/locuslink:.*',
            'refseq:[\\w\\d_-]*$'
        ]

        # If we're dealing with multiple identifiers separated by a pipe.
        if '|' in row_entry:
            row_entries = row_entry.split('|')
        else:
            row_entries = [row_entry]

        for individual_entry in row_entries:

            # For use in wormbase / flybase lookups.
            # If we run into an IndexError, there's no identifier to resolve and we return False.
            # All valid identifiers in the PSI-MI TAB file should be "splittable".
            try:
                entry_stripped = individual_entry.split(':')[1]
            except IndexError:
                return None

            if individual_entry.startswith('uniprotkb:'):
                individual_entry = individual_entry.split('-')[0]

            prefixed_identifier = None

            # TODO implement regex for WB / FB gene identifiers.
            if entry_stripped.startswith('WB'):
                prefixed_identifier = 'WB:' + entry_stripped
                if prefixed_identifier in master_gene_set:
                    return [prefixed_identifier]  # Always return a list for later processing.
                return None
            # TODO implement regex for WB / FB gene identifiers.
            elif entry_stripped.startswith('FB'):
                prefixed_identifier = 'FB:' + entry_stripped
                if prefixed_identifier in master_gene_set:
                    return [prefixed_identifier]  # Always return a list for later processing.
                return None

            for regex_entry in list_of_crossref_regex_to_search:
                regex_output = re.findall(regex_entry, individual_entry)
                if regex_output is not None:
                    # We might have multiple regex matches.
                    # Search them all against our crossreferences.
                    for regex_match in regex_output:
                        identifier = regex_match
                        for crossreference_type in master_crossreference_dictionary.keys():
                            # Using lowercase in the identifier to be consistent
                            # with Alliance lowercase identifiers.
                            if identifier.lower() in \
                                     master_crossreference_dictionary[crossreference_type]:
                                # Return the corresponding Alliance gene(s).
                                return master_crossreference_dictionary[crossreference_type][identifier.lower()]
        # If we can't resolve any of the crossReferences, return None

        # print('Could not resolve identifiers.')
        # print(row_entries)

        return None

    def publication_search(self, row_entry):
        found_match = False
        publication_url = None
        publication = None
        list_of_possible_pub_parameters = [
            (r'pubmed:\d+', 'pubmed', 'PMID'),
            (r'^(DOI:)?\d{2}\.\d{4}.*$', 'DOI', 'doi'),
            (r'^flybase:FBrf\d+', 'flybase', 'FB')
        ]

        for check in list_of_possible_pub_parameters:
            publication_re = re.search(check[0], row_entry, re.IGNORECASE)
            if publication_re is not None:
                publication = publication_re.group(0)  # matching bit
                publication = publication.replace(check[1], check[2])
                publication_url = self.etlh.rdh2.return_url_from_identifier(publication)
                found_match = True
                break
            else:
                continue

        return found_match, publication_url, publication

    def get_generators(self, filepath, batch_size):  # noqa
        """Get Generators."""
        list_to_yield = []
        xref_list_to_yield = []
        mod_xref_list_to_yield = []

        # TODO Taxon species needs to be pulled out into a standalone
        # module to be used by other scripts.
        # TODO External configuration script for these types of filters?
        # Not a fan of hard-coding.

        # Populate our master dictionary for resolving cross references.
        master_crossreference_dictionary = self.populate_crossreference_dictionary()
        self.logger.info('Obtained the following number of cross references from Neo4j:')
        for entry in master_crossreference_dictionary:
            self.logger.info('%s: %s', entry, len(master_crossreference_dictionary[entry]))

        # Populate our master gene set for filtering Alliance genes.
        master_gene_set = self.populate_genes()
        self.logger.info('Obtained %s gene primary ids from Neo4j.', len(master_gene_set))

        resolved_a_b_count = 0
        unresolved_a_b_count = 0
        total_interactions_loaded_count = 0
        unresolved_publication_count = 0

        # Used for debugging.
        # unresolved_entries = []
        # unresolved_crossref_set = set()

        self.logger.info('Attempting to open %s', filepath)
        with open(filepath, 'r', encoding='utf-8') as tsvin:
            tsvin = csv.reader(tsvin, delimiter='\t')
            counter = 0
            total_counter = 0
            for row in tsvin:
                counter += 1
                total_counter += 1
                if total_counter % 100000 == 0:
                    self.logger.info('Processing row %s.', total_counter)

                # Skip commented rows.
                if row[0].startswith('#'):
                    continue

                taxon_id_1 = row[9]
                taxon_id_2 = row[10]

                # After we pass all our filtering / continue opportunities,
                # we start working with the variables.
                taxon_id_1_re = re.search(r'\d+', taxon_id_1)
                taxon_id_1_to_load = 'NCBITaxon:' + taxon_id_1_re.group(0)

                taxon_id_2_to_load = None
                if taxon_id_2 != '-':
                    taxon_id_2_re = re.search(r'\d+', taxon_id_2)
                    taxon_id_2_to_load = 'NCBITaxon:' + taxon_id_2_re.group(0)
                else:
                    taxon_id_2_to_load = taxon_id_1_to_load  # self interaction

                try:
                    # Interactor ID for the UI table
                    identifier_linkout_list = self.process_interaction_identifier(row[13], row[24])
                except IndexError:
                    # Interactor ID for the UI table
                    identifier_linkout_list = self.process_interaction_identifier(row[13], None)
                source_database = None

                # grab the MI identifier between two quotes ""
                source_database = re.findall(r'"([^"]*)"', row[12])[0]

                # database_linkout_set.add(source_database)

                aggregation_database = 'MI:0670'  # IMEx

                if source_database == 'MI:0478':  # FlyBase
                    aggregation_database = 'MI:0478'
                elif source_database == 'MI:0487':  # WormBase
                    aggregation_database = 'MI:0487'
                elif source_database == 'MI:0463':  # BioGRID
                    aggregation_database = 'MI:0463'

                detection_method = 'MI:0686'  # Default to unspecified.
                try:
                    # grab the MI identifier between two quotes ""
                    detection_method = re.findall(r'"([^"]*)"', row[6])[0]
                except IndexError:
                    pass  # Default to unspecified, see above.

                if row[8] != '-':
                    found_match, publication_url, publication = self.publication_search(row[8])
                    if found_match is False:
                        unresolved_publication_count += 1
                        continue
                else:
                    unresolved_publication_count += 1
                    continue

                # Other hardcoded values to be used for now.
                interactor_a_role = 'MI:0499'  # Default to unspecified.
                interactor_b_role = 'MI:0499'  # Default to unspecified.
                interactor_a_type = 'MI:0499'  # Default to unspecified.
                interactor_b_type = 'MI:0499'  # Default to unspecified.

                try:
                    interactor_a_role = re.findall(r'"([^"]*)"', row[18])[0]
                except IndexError:
                    pass  # Default to unspecified, see above.
                try:
                    interactor_b_role = re.findall(r'"([^"]*)"', row[19])[0]
                except IndexError:
                    pass  # Default to unspecified, see above.

                try:
                    interactor_a_type = re.findall(r'"([^"]*)"', row[20])[0]
                except IndexError:
                    pass  # Default to unspecified, see above.

                try:
                    interactor_b_type = re.findall(r'"([^"]*)"', row[21])[0]
                except IndexError:
                    pass  # Default to unspecified, see above.

                interaction_type = None
                interaction_type = re.findall(r'"([^"]*)"', row[11])[0]

                interactor_a_resolved = None
                interactor_b_resolved = None

                interactor_a_resolved, interactor_b_resolved = self.resolve_identifiers_by_row(
                    row,
                    master_gene_set,
                    master_crossreference_dictionary)

                if interactor_a_resolved is None or interactor_b_resolved is None:
                    unresolved_a_b_count += 1  # Tracking unresolved identifiers.

                    # Uncomment the line below for debugging.
                    # unresolved_entries.append([row[0], interactor_a_resolved, row[1], interactor_b_resolved, row[8]])
                    # if interactor_a_resolved is None:
                    #     unresolved_crossref_set.add(row[0])
                    # if interactor_b_resolved is None:
                    #     unresolved_crossref_set.add(row[1])

                    continue  # Skip this entry.

                mol_int_dataset = {
                    'interactor_A': None,
                    'interactor_B': None,
                    'interactor_A_type': interactor_a_type,
                    'interactor_B_type': interactor_b_type,
                    'interactor_A_role': interactor_a_role,
                    'interactor_B_role': interactor_b_role,
                    'interaction_type': interaction_type,
                    'taxon_id_1': taxon_id_1_to_load,
                    'taxon_id_2': taxon_id_2_to_load,
                    'detection_method': detection_method,
                    'pub_med_id': publication,
                    'pub_med_url': publication_url,
                    'uuid': None,
                    'source_database': source_database,
                    'aggregation_database':  aggregation_database
                }

                # Remove possible duplicates from interactor lists.
                interactor_a_resolved_no_dupes = list(set(interactor_a_resolved))
                interactor_b_resolved_no_dupes = list(set(interactor_b_resolved))

                # Get every possible combination of interactor A x interactor B
                # (if multiple ids resulted from resolving the identifier.)
                int_combos = list(itertools.product(interactor_a_resolved_no_dupes,
                                                    interactor_b_resolved_no_dupes))

                # Update the dictionary with every possible combination of
                # interactor A x interactor B.
                list_of_mol_int_dataset = [dict(mol_int_dataset,
                                                interactor_A=x,
                                                interactor_B=y,
                                                uuid=str(uuid.uuid4())) for x, y in int_combos]
                # Tracking successfully loaded identifiers.
                total_interactions_loaded_count += len(list_of_mol_int_dataset)
                # Tracking successfully resolved identifiers.
                resolved_a_b_count += 1
                # We need to also create new crossreference dicts for every
                # new possible interaction combination.
                new_identifier_linkout_list = []
                for dataset_entry in list_of_mol_int_dataset:
                    for identifier_linkout in identifier_linkout_list:
                        new_identifier_linkout_list.append(
                            dict(identifier_linkout,
                                 reference_uuid=dataset_entry['uuid']))

                # Create dictionaries for xrefs from Alliance genes
                # to MOD interaction sections of gene reports.
                for primary_gene_to_link in interactor_a_resolved_no_dupes:
                    # We have the potential for numerous duplicate xrefs.
                    # Check whether we've made this xref previously by looking in a list.
                    # Should cut down loading time for Neo4j significantly.
                    # Hopefully the lookup is not too long -- this should be refined if it's slow.
                    # Ignore ZFIN interaction pages and REFSEQ.
                    if not primary_gene_to_link.startswith('ZFIN') and not primary_gene_to_link.startswith('RefSeq'):
                        if primary_gene_to_link not in self.successful_mod_interaction_xrefs:
                            mod_xref_dataset = self.add_mod_interaction_links(primary_gene_to_link)
                            mod_xref_list_to_yield.append(mod_xref_dataset)

                # Establishes the number of entries to yield (return) at a time.
                xref_list_to_yield.extend(new_identifier_linkout_list)
                list_to_yield.extend(list_of_mol_int_dataset)

                if counter == batch_size:
                    counter = 0
                    yield list_to_yield, xref_list_to_yield, mod_xref_list_to_yield
                    list_to_yield = []
                    xref_list_to_yield = []
                    mod_xref_list_to_yield = []

            if counter > 0:
                yield list_to_yield, xref_list_to_yield, mod_xref_list_to_yield

        # TODO Clean up the set output.
        # for entry in unresolved_entries:
        #     self.logger.info(*entry)

        # self.logger.info('A set of unique unresolvable cross references:')
        # for unique_entry in unresolved_crossref_set:
        #     self.logger.info(unique_entry)

        self.logger.info('Resolved identifiers for %s PSI-MITAB interactions.',
                         resolved_a_b_count)
        self.logger.info('Prepared to load %s total interactions %s.',
                         total_interactions_loaded_count,
                         '(accounting for multiple possible identifier resolutions)')

        self.logger.info('Note: Interactions missing valid publications will be skipped, even if their identifiers'
                         ' resolve correctly.')

        self.logger.info('Could not resolve [and subsequently will not load] '
                         '{} interactions due to missing publications.'.format(unresolved_publication_count))

        self.logger.info('Could not resolve [and subsequently will not load] %s interactions due to unresolved'
                         ' identifiers.',
                         unresolved_a_b_count)
