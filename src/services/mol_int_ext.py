from files import S3File, TARFile
import uuid, csv, re, sys
import pprint, itertools
from services import ResourceDescriptorService
from transactions import Transaction
import logging

logger = logging.getLogger(__name__)

class MolIntExt(object):

    def __init__(self):
        # Initialize an instance of ResourceDescriptor for processing external links.
        self.resource_descriptor_dict = ResourceDescriptorService()
        self.missed_database_linkouts = set()
        self.successful_database_linkouts = set()
        self.ignored_database_linkouts = set()

    def populate_genes(self):

        master_gene_set = set()

        query = "MATCH (g:Gene) RETURN g.primaryKey"

        result = Transaction().run_single_query(query)

        for record in result:
            master_gene_set.add(record['g.primaryKey'])

        return master_gene_set

    def query_crossreferences(self, crossref_prefix):
        query = "MATCH (g:Gene)-[C:CROSS_REFERENCE]-(cr:CrossReference) WHERE cr.prefix = {parameter} RETURN g.primaryKey, cr.globalCrossRefId"
        return Transaction().run_single_parameter_query(query, crossref_prefix)

    def populate_crossreference_dictionary(self):
        # We're populating a rather large dictionary to use for looking up Alliance genes by their crossreferences.
        # Edit the list below if you'd like to add more crossreferences to the dictionary.
        # The key of the dictionary is the crossreference and the value is the Alliance gene to which it resolves.

        master_crossreference_dictionary = dict()

        master_crossreference_dictionary['UniProtKB'] = dict()
        master_crossreference_dictionary['ENSEMBL'] = dict()
        master_crossreference_dictionary['NCBI_Gene'] = dict()

        for key in master_crossreference_dictionary.keys():
            logger.info('Querying for %s cross references.' % (key))
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

                # The crossreference dictionary is a list of genes linked to a single crossreference.
                # Append the gene if the crossref dict entry exists. Otherwise, create a list and append the entry.
                if cross_ref_record.lower() in master_crossreference_dictionary[key]:
                    master_crossreference_dictionary[key][cross_ref_record.lower()].append(record['g.primaryKey'])
                else:
                    master_crossreference_dictionary[key][cross_ref_record.lower()] = []
                    master_crossreference_dictionary[key][cross_ref_record.lower()].append(record['g.primaryKey'])

                # The ids in PSI-MITAB files are lower case, hence the .lower() used above.

        return master_crossreference_dictionary

    def process_interaction_identifier(self, entry, additional_row):
        # Create cross references for all the external identifiers.

        xref_main_list = []
        entries = None

        # Identifier types on this list DO NOT receive a crossRefCompleteUrl field for external linking.
        ignored_identifier_database_list = [
            'brenda', # Not currently required.
            'cell ontology', # Not currently required.
            'chebi', # Not currently required
            'chembl compound', # Not currently required.
            'efo', # Not currently required.
            'flannotator', # Not currently required.
            'intenz', # Not currently required.
            'interpro', # Not currently required.
            'mpidb', # Not currently required.
            'omim', # Not currently required.
            'pdbj', # Not currently required.
            'pmc', # Not currently required.
            'pride', # Not currently required.
            'prints', # Not currently required.
            'proteomexchange', # Not currently required.
            'psi-mi', # Not currently required.
            'pubmed', # Not currently required.
            'go', # Not currently required.
            'reactome', # Not currently required.
            'tissue list', # Not currently required.
            'uniprotkb' # Not currently required.
        ]

        if '|' in entry:
            entries = entry.split('|')
        else:
            entries = [entry]

        for individual in entries:

            xref_dict = {}
            page = 'gene/interactions'

            individual_prefix, individual_body, separator = self.resource_descriptor_dict.split_identifier(individual)
            # Capitalize the prefix to match the YAML and change the prefix if necessary to match the YAML.
            xref_dict['prefix'] = individual_prefix
            xref_dict['localId'] = individual_body

            # Special case for dealing with FlyBase.
            # The identifier link needs to use row 25 from the psi-mitab file.
            # TODO Regex to check for FBig in additional_row?
            if individual.startswith('flybase:FBrf'):
                if '|' in additional_row:
                    individual = additional_row.split('|')[0]
                else:
                    individual = additional_row
                
                regex_check = re.match('^flybase:FBig\\d{10}$', individual)
                if regex_check is None:
                    logger.critical('Fatal Error: During special handling of FlyBase molecular interaction links, an FBig ID was not found.')
                    logger.critical('Failed identifier: %s' % (individual))
                    logger.critical('PSI-MITAB row entry: %s' % (additional_row))
                    sys.exit(-1)

            if not individual.startswith(tuple(ignored_identifier_database_list)):
                try: 
                    individual_url = self.resource_descriptor_dict.return_url(individual, page)
                    xref_dict['crossRefCompleteUrl'] = individual_url
                    self.successful_database_linkouts.add(individual_prefix)
                except KeyError:
                    self.missed_database_linkouts.add(individual_prefix)
            else: self.ignored_database_linkouts.add(individual_prefix)

            xref_dict['uuid'] = str(uuid.uuid4())
            xref_dict['globalCrossRefId'] = individual
            xref_dict['name'] = individual
            xref_dict['displayName'] = individual_body
            xref_dict['primaryKey'] = individual
            xref_dict['crossRefType'] = 'interaction'
            xref_dict['page'] = page
            xref_dict['reference_uuid'] = None # For association interactions (later).

            # Special case for FlyBase as "individual" is not unique in their case.
            # Individual_body needs to be used instead.
            
            if individual.startswith('flybase'):
                xref_dict['primaryKey'] = individual_body
            xref_main_list.append(xref_dict)

        return xref_main_list

    def resolve_identifiers_by_row(self, row, master_gene_set, master_crossreference_dictionary):
        interactor_A_rows = [0, 2, 4, 22]
        interactor_B_rows = [1, 3, 5, 23]

        interactor_A_resolved = None
        interactor_B_resolved = None

        for row_entry in interactor_A_rows:
            try:
                interactor_A_resolved = self.resolve_identifier(row[row_entry], master_gene_set, master_crossreference_dictionary)
                if interactor_A_resolved is not None:
                    break
            except IndexError: # Biogrid has less rows than other files, continue on IndexErrors.
                continue

        for row_entry in interactor_B_rows:
            try:
                interactor_B_resolved = self.resolve_identifier(row[row_entry], master_gene_set, master_crossreference_dictionary)
                if interactor_B_resolved is not None:
                    break
            except IndexError: # Biogrid has less rows than other files, continue on IndexErrors.
                continue

        return interactor_A_resolved, interactor_B_resolved

    def resolve_identifier(self, row_entry, master_gene_set, master_crossreference_dictionary):
        
        list_of_crossref_regex_to_search = [
            'uniprotkb:[\\w\\d_-]*$',
            'ensembl:[\\w\\d_-]*$',
            'entrez gene/locuslink:.*'
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

            prefixed_identifier = None

            if entry_stripped.startswith('WB'): # TODO implement regex for WB / FB gene identifiers.
                prefixed_identifier = 'WB:' + entry_stripped
                if prefixed_identifier in master_gene_set:
                    return [prefixed_identifier] # Always return a list for later processing.
                else:
                    return None
            elif entry_stripped.startswith('FB'): # TODO implement regex for WB / FB gene identifiers.
                prefixed_identifier = 'FB:' + entry_stripped
                if prefixed_identifier in master_gene_set:
                    return [prefixed_identifier] # Always return a list for later processing.
                else:
                    return None

            for regex_entry in list_of_crossref_regex_to_search:
                regex_output = re.findall(regex_entry, individual_entry)
                if regex_output is not None:
                    for regex_match in regex_output: # We might have multiple regex matches. Search them all against our crossreferences.
                        identifier = regex_match
                        for crossreference_type in master_crossreference_dictionary.keys():
                            # Using lowercase in the identifier to be consistent with Alliance lowercase identifiers.
                            if identifier.lower() in master_crossreference_dictionary[crossreference_type]:
                                return master_crossreference_dictionary[crossreference_type][identifier.lower()] # Return the corresponding Alliance gene(s).

        # If we can't resolve any of the crossReferences, return None
        return None            

    def get_data(self, batch_size):
        path = 'tmp'
        filename = 'Alliance_molecular_interactions_2.0.txt'
        filename_comp = 'INT/Alliance_molecular_interactions_2.0.tar.gz'

        S3File(filename_comp, path).download()
        TARFile(path, filename_comp).extract_all()

        list_to_yield = []
        xref_list_to_yield = []

        # TODO Taxon species needs to be pulled out into a standalone module to be used by other scripts. 
        # TODO External configuration script for these types of filters? Not a fan of hard-coding.

        # Populate our master dictionary for resolving cross references.
        master_crossreference_dictionary = self.populate_crossreference_dictionary()

        # Populate our master gene set for filtering Alliance genes.
        master_gene_set = self.populate_genes()

        resolved_a_b_count = 0
        unresolved_a_b_count = 0
        total_interactions_loaded_count = 0
        pp = pprint.PrettyPrinter(indent=4)
        counter = 0

        database_linkout_set = set()

        with open(path + "/" + filename, 'r', encoding='utf-8') as tsvin:
            tsvin = csv.reader(tsvin, delimiter='\t')
            for row in tsvin:
                
                # Skip commented rows.
                if row[0].startswith('#'):
                    continue
                
                taxon_id_1 = row[9]
                taxon_id_2 = row[10]

                # After we pass all our filtering / continue opportunities, we start working with the variables.
                taxon_id_1_re = re.search('\d+', taxon_id_1)
                taxon_id_1_to_load = 'NCBITaxon:' + taxon_id_1_re.group(0)

                taxon_id_2_to_load = None
                if taxon_id_2 is not '-':
                    taxon_id_2_re = re.search('\d+', taxon_id_2)
                    taxon_id_2_to_load = 'NCBITaxon:' + taxon_id_2_re.group(0)
                else:
                    taxon_id_2_to_load = taxon_id_1_to_load # self interaction
                
                try: 
                    identifier_linkout_list = self.process_interaction_identifier(row[13], row[24]) # Interactor ID for the UI table
                except IndexError:
                    identifier_linkout_list = self.process_interaction_identifier(row[13], None) # Interactor ID for the UI table

                source_database = None
                source_database = re.findall(r'"([^"]*)"', row[12])[0] # grab the MI identifier between two quotes ""

                database_linkout_set.add(source_database)

                aggregation_database = 'MI:0670' # IMEx

                if source_database == 'MI:0478': # FlyBase
                    aggregation_database = 'MI:0478'
                elif source_database == 'MI:0487': # WormBase
                    aggregation_database = 'MI:0487'
                elif source_database == 'MI:0463': # BioGRID
                    aggregation_database = 'MI:0463'
                    
                detection_method = 'MI:0686' # Default to unspecified.
                try: 
                    detection_method = re.findall(r'"([^"]*)"', row[6])[0] # grab the MI identifier between two quotes ""
                except IndexError:
                    pass # Default to unspecified, see above.

                # TODO Replace this publication work with a service. Re-think publication implementation in Neo4j.
                publication = None
                publication_url = None
                
                if row[8] is not '-':
                    publication_re = re.search('pubmed:\d+', row[8])
                    if publication_re is not None:
                        publication = publication_re.group(0)
                        publication = publication.replace('pubmed', 'PMID')
                        publication_url = 'https://www.ncbi.nlm.nih.gov/pubmed/%s' % (publication[5:])
                    else:
                        continue
                else:
                    continue

                # Other hardcoded values to be used for now.
                interactor_A_role = 'MI:0499' # Default to unspecified.
                interactor_B_role = 'MI:0499' # Default to unspecified.
                interactor_A_type = 'MI:0499' # Default to unspecified.
                interactor_B_type = 'MI:0499' # Default to unspecified.
                
                try:
                    interactor_A_role = re.findall(r'"([^"]*)"', row[18])[0]
                except IndexError:
                    pass # Default to unspecified, see above.
                try:
                    interactor_B_role = re.findall(r'"([^"]*)"', row[19])[0]
                except IndexError:
                    pass # Default to unspecified, see above.
                
                try:
                    interactor_A_type = re.findall(r'"([^"]*)"', row[20])[0]
                except IndexError:
                    pass # Default to unspecified, see above.

                try:
                    interactor_B_type = re.findall(r'"([^"]*)"', row[21])[0]
                except IndexError:
                    pass # Default to unspecified, see above.

                interaction_type = None
                interaction_type = re.findall(r'"([^"]*)"', row[11])[0]

                interactor_A_resolved = None
                interactor_B_resolved = None

                interactor_A_resolved, interactor_B_resolved = self.resolve_identifiers_by_row(row, master_gene_set, master_crossreference_dictionary)

                if interactor_A_resolved is None or interactor_B_resolved is None:
                    unresolved_a_b_count += 1 # Tracking unresolved identifiers.
                    continue # Skip this entry.
            
                mol_int_dataset = {
                    'interactor_A' : None,
                    'interactor_B' : None,
                    'interactor_A_type' : interactor_A_type,
                    'interactor_B_type' : interactor_B_type,
                    'interactor_A_role' : interactor_A_role,
                    'interactor_B_role' : interactor_B_role,
                    'interaction_type' : interaction_type,
                    'taxon_id_1' : taxon_id_1_to_load,
                    'taxon_id_2' : taxon_id_2_to_load,
                    'detection_method' : detection_method,
                    'pub_med_id' : publication,
                    'pub_med_url' : publication_url,
                    'uuid' : None,
                    'source_database' : source_database,
                    'aggregation_database' :  aggregation_database
                }

                # Remove possible duplicates from interactor lists.
                interactor_A_resolved_no_dupes = list(set(interactor_A_resolved))
                interactor_B_resolved_no_dupes = list(set(interactor_B_resolved))

                # Get every possible combination of interactor A x interactor B (if multiple ids resulted from resolving the identifier.)
                int_combos = list(itertools.product(interactor_A_resolved_no_dupes, interactor_B_resolved_no_dupes))

                # Update the dictionary with every possible combination of interactor A x interactor B.
                list_of_mol_int_dataset = [dict(mol_int_dataset, interactor_A=x, interactor_B=y, uuid=str(uuid.uuid4())) for x,y in int_combos]
                total_interactions_loaded_count += len(list_of_mol_int_dataset) # Tracking successfully loaded identifiers.
                resolved_a_b_count += 1 # Tracking successfully resolved identifiers.

                # We need to also create new crossreference dicts for every new possible interaction combination.
                new_identifier_linkout_list = []
                for dataset_entry in list_of_mol_int_dataset:  
                    for identifier_linkout in identifier_linkout_list:
                        new_identifier_linkout_list.append(dict(identifier_linkout, reference_uuid=dataset_entry['uuid']))
                
                counter+=1

                # Establishes the number of entries to yield (return) at a time.
                xref_list_to_yield.extend(new_identifier_linkout_list)
                list_to_yield.extend(list_of_mol_int_dataset)
                if counter == batch_size:
                    counter = 0
                    yield list_to_yield, xref_list_to_yield
                    list_to_yield = []
                    xref_list_to_yield = []
            
            if counter > 0:
                yield list_to_yield, xref_list_to_yield

        # TODO Change this to log printing and clean up the set output.
        logger.info('Resolved identifiers for %s PSI-MITAB interactions.' % resolved_a_b_count)
        logger.info('Prepared to load %s total interactions (accounting for multiple possible identifier resolutions).' % total_interactions_loaded_count)
        logger.info('Successfully created linkouts for the following identifier databases:')
        pp.pprint(self.successful_database_linkouts)

        logger.info('Could not resolve [and subsequently will not load] %s interactions' % unresolved_a_b_count)
        logger.info('Could not create linkouts for the following identifier databases:')
        pp.pprint(self.missed_database_linkouts)

        logger.info('The following linkout databases were ignored:')
        pp.pprint(self.ignored_database_linkouts)
