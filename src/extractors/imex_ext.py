from files import S3File, ZIPFile
import uuid, csv, re
import urllib.request, json

class IMEXExt(object):

    def get_data(self, batch_size):
        path = 'tmp'
        filename = '/intact.txt'
        filename_comp = 'intact.zip'

        #S3File("mod-datadumps/IMEX", filename_comp, path).download()
        ZIPFile(path, filename_comp).extract_all()

        list_to_yield = []

        with open(path + filename, 'r', encoding='utf-8') as tsvin:
            tsvin = csv.reader(tsvin, delimiter='\t')
            next(tsvin, None) # Skip the headers

            # TODO Taxon species needs to be pulled out into a standalone module to be used by other scripts. 
            # TODO External configuration script for these types of filters? Not a fan of hard-coding.
            taxon_species_set = ('taxid:10116', 'taxid:9606', 'taxid:10090', 'taxid:6239', 'taxid:559292', 'taxid:7955', '-')
            interaction_exclusion_set = ('psi-mi:\"MI:0208\"')
            interactor_type_exclusion_set = ('psi-mi:\"MI:0328\"', 'psi-mi:\"MI:1302\"', 'psi-mi:\"MI:1304\"')

            for row in tsvin:
                taxon_id_1 = row[9]
                taxon_id_2 = row[10]

                if not taxon_id_1.startswith(taxon_species_set) or not taxon_id_2.startswith(taxon_species_set):
                    continue # Skip rows where we don't have Alliance species or a blank entry.
                if row[0].startswith('uniprotkb') and row[1].startswith('uniprotkb'):
                    try:
                        interactor_one = row[0].split(':')[1]
                        interactor_two = row[1].split(':')[1]
                    except:
                        IndexError() # Skipping cases where we don't find something like 'uniprot:identifier'.
                        continue
                else: # If we don't have uniprot ids, continue.
                    continue

                if row[11].startswith(interaction_exclusion_set):
                    continue

                if row[12].startswith(interactor_type_exclusion_set):
                    continue

                if row[15] is not '-':
                    continue

                # After we pass all our filtering / continue opportunities, we start working with the variables.
                taxon_id_1_re = re.search('\d+', taxon_id_1)
                taxon_id_1_to_load = 'NCBITaxon:' + taxon_id_1_re.group(0)

                taxon_id_2_to_load = None
                if taxon_id_2 is not '-':
                    taxon_id_2_re = re.search('\d+', taxon_id_2)
                    taxon_id_2_to_load = 'NCBITaxon:' + taxon_id_2_re.group(0)
                else:
                    taxon_id_2_to_load = taxon_id_1_to_load # self interaction

                detection_method_re = re.search('"([^"]*)"', row[6]) # grab the MI identifier between two quotes ""
                detection_method = detection_method_re.group(0)
                detection_method = re.sub('\"', '', detection_method) # TODO Fix the regex capture above to remove this step.

                publication_re = re.search('pubmed:\d+', row[8])
                publication = publication_re.group(0)


                # Publication requires "pubMedId" : "PMID:1234"
                # "pubMedUrl" : "https://www.ncbi.nlm.nih.gov/pubmed/11358670"
                # "primaryKey" : "PMID:1234"

                imex_dataset = {
                    'interactor_one' : interactor_one,
                    'interactor_two' : interactor_two,
                    'taxon_id_1' : taxon_id_1_to_load,
                    'taxon_id_2' : taxon_id_2_to_load,
                    'detection_method' : detection_method
                }

            # Establishes the number of entries to yield (return) at a time.
            list_to_yield.append(imex_dataset)
            if len(list_to_yield) == batch_size:
                yield list_to_yield
                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield