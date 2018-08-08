from files import S3File, TARFile
import uuid, csv, re
import urllib.request, json

class MolIntExt(object):

    def get_data(self, batch_size):
        path = 'tmp'
        filename = 'Alliance_molecular_interactions.txt'
        filename_comp = 'INT/Alliance_molecular_interactions.tar.gz'

        S3File(filename_comp, path).download()
        TARFile(path, filename_comp).extract_all()

        list_to_yield = []

        # TODO Taxon species needs to be pulled out into a standalone module to be used by other scripts. 
        # TODO External configuration script for these types of filters? Not a fan of hard-coding.

        with open(path + "/" + filename, 'r', encoding='utf-8') as tsvin:
            tsvin = csv.reader(tsvin, delimiter='\t')
            next(tsvin, None) # Skip the headers

            for row in tsvin:
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

                detection_method_re = re.search('"([^"]*)"', row[6]) # grab the MI identifier between two quotes ""
                detection_method = detection_method_re.group(0)
                detection_method = re.sub('\"', '', detection_method) # TODO Fix the regex capture above to remove this step.

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
                interactor_type = 'protein' # TODO Use MI ontology or query from psi-mitab?
                molecule_type = 'protein' # TODO Use MI ontology or query from psi-mitab?

                interactor_one = None
                interactor_two = None

                imex_dataset = {
                    'interactor_one' : interactor_one,
                    'interactor_two' : interactor_two,
                    'interactor_type' : interactor_type,
                    'molecule_type' : molecule_type,
                    'taxon_id_1' : taxon_id_1_to_load,
                    'taxon_id_2' : taxon_id_2_to_load,
                    'detection_method' : detection_method,
                    'pub_med_id' : publication,
                    'pub_med_url' : publication_url,
                    'uuid' : str(uuid.uuid4())
                }
                
                # Establishes the number of entries to yield (return) at a time.
                list_to_yield.append(imex_dataset)
                if len(list_to_yield) == batch_size:
                    yield list_to_yield
                    list_to_yield[:] = []  # Empty the list.

            if len(list_to_yield) > 0:
                yield list_to_yield