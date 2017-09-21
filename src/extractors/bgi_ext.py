import uuid

class BGIExt(object):

    def get_data(self, gene_data, batch_size, testObject):

        gene_dataset = {}
        list_to_yield = []

        dateProduced = gene_data['metaData']['dateProduced']
        dataProvider = gene_data['metaData']['dataProvider']
        release = None

        if 'release' in gene_data['metaData']:
            release = gene_data['metaData']['release']

        for geneRecord in gene_data['data']:
            crossReferences = []
            genomic_locations = []

            primary_id = geneRecord['primaryId']
            global_id = geneRecord['primaryId']

            local_id = global_id.split(":")[1]

            modCrossReference = {
                "id": global_id, 
                "globalCrossRefId": global_id, 
                "localId": local_id, 
                "crossrefCompleteUrl": self.get_complete_url(local_id, global_id, primary_id)
            }

            if geneRecord['taxonId'] == "NCBITaxon:9606" or geneRecord['taxonId'] == "NCBITaxon:10090":
                local_id = geneRecord['primaryId']

            if testObject.using_test_data() is True:
                is_it_test_entry = testObject.check_for_test_id_entry(primary_id)
                if is_it_test_entry is False:
                    continue

            if 'crossReferenceIds' in geneRecord:
                for crossRef in geneRecord['crossReferenceIds']:
                    #this can be simplified when GO YAML reused for AGR has helper fields.
                    if ':' in crossRef:
                        local_crossref_id = crossRef.split(":")[1]
                        prefix = crossRef.split(":")[0]
                        crossRefPrimaryId = None
                        if prefix == 'PANTHER': # special Panther case to be addressed post 1.0
                            crossRefPrimaryId = crossRef + '_' + primary_id
                        else:
                            crossRefPrimaryId = crossRef
                        crossReferences.append({
                            "id": crossRefPrimaryId, 
                            "globalCrossRefId": crossRef,
                            "localId": local_crossref_id, 
                            "crossRefCompleteUrl": self.get_complete_url(local_crossref_id, crossRef, primary_id),
                            "prefix": crossRef.split(":")[0]
                            })
                    else:
                        local_crossref_id = crossRef
                        crossReferences.append(
                            {"id": crossRefPrimaryId, "globalCrossRefId": crossRef, "localId": local_crossref_id,
                             "crossRefCompleteUrl": self.get_complete_url(local_crossref_id, crossRef, primary_id), "prefix": prefix})
            if 'genomeLocations' in geneRecord:
                for genomeLocation in geneRecord['genomeLocations']:
                    chromosome = genomeLocation['chromosome']
                    assembly = genomeLocation['assembly']
                    if 'startPosition' in genomeLocation:
                        start = genomeLocation['startPosition']
                    else:
                        start = None
                    if 'endPosition' in genomeLocation:
                        end = genomeLocation['endPosition']
                    else:
                        end = None
                    if 'strand' in geneRecord['genomeLocations']:
                        strand = genomeLocation['strand']
                    else:
                        strand = None
                    genomic_locations.append(
                        {"geneLocPrimaryId": primary_id, "chromosome": chromosome, "start": start, "end": end, "strand": strand, "assembly": assembly})

            gene_dataset = {
                "symbol": geneRecord['symbol'],
                "name": geneRecord.get('name'),
                "geneticEntityExternalUrl": self.get_complete_url(local_id,global_id,primary_id),
                "description": geneRecord.get('description'),
                "synonyms": geneRecord.get('synonyms'),
                "soTermId": geneRecord['soTermId'],
                "soTermName": None,
                "diseases": [],
                "secondaryIds": geneRecord.get('secondaryIds'),
                "geneSynopsis": geneRecord.get('geneSynopsis'),
                "geneSynopsisUrl": geneRecord.get('geneSynopsisUrl'),
                "taxonId": geneRecord['taxonId'],
                "species": self.get_species(geneRecord['taxonId']),
                "genomeLocations": genomic_locations,
                "geneLiteratureUrl": geneRecord.get('geneLiteratureUrl'),
                "name_key": geneRecord['symbol'],
                "primaryId": primary_id,
                "crossReferences": crossReferences,
                "modCrossReference": modCrossReference,
                "category": "gene",
                "dateProduced": dateProduced,
                "dataProvider": dataProvider,
                "release": release,
                "href": None,
                "uuid": str(uuid.uuid1()),
                "modCrossRefCompleteUrl": self.get_complete_url(local_id, global_id,primary_id),
                "localId": local_id,
                "modGlobalCrossRefId": global_id,
                "modGlobalId": global_id,
                "loadKey": dataProvider+"_"+dateProduced+"_BGI"
            }

            
            # Establishes the number of genes to yield (return) at a time.
            list_to_yield.append(gene_dataset)
            if len(list_to_yield) == batch_size:
                yield list_to_yield
                list_to_yield[:] = []  # Empty the list.

        if len(list_to_yield) > 0:
            yield list_to_yield

    def get_species(self, taxon_id):
        if taxon_id in ("NCBITaxon:7955"):
            return "Danio rerio"
        elif taxon_id in ("NCBITaxon:6239"):
            return "Caenorhabditis elegans"
        elif taxon_id in ("NCBITaxon:10090"):
            return "Mus musculus"
        elif taxon_id in ("NCBITaxon:10116"):
            return "Rattus norvegicus"
        elif taxon_id in ("NCBITaxon:559292"):
            return "Saccharomyces cerevisiae"
        elif taxon_id in ("NCBITaxon:7227"):
            return "Drosophila melanogaster"
        elif taxon_id in ("NCBITaxon:9606"):
            return "Homo sapiens"
        else:
            return None

    def get_complete_url (self, local_id, global_id, primary_id):
        # Local and global are cross references, primary is the gene id.
        # TODO Update to dispatch?
        complete_url = None
        panther_url = None
        split_primary = None

        if global_id.startswith('MGI'):
            complete_url = 'http://www.informatics.jax.org/accession/' + global_id
        elif global_id.startswith('RGD'):
            complete_url = 'http://rgd.mcw.edu/rgdweb/search/search.html?term=' + local_id
        elif global_id.startswith('SGD'):
            complete_url = 'http://www.yeastgenome.org/locus/' + local_id
        elif global_id.startswith('FB'):
            complete_url = 'http://flybase.org/reports/' + local_id + '.html'
        elif global_id.startswith('ZFIN'):
            complete_url = 'http://zfin.org/' + local_id
        elif global_id.startswith('WB:'):
            complete_url = 'http://www.wormbase.org/species/c_elegans/gene/' + local_id
        elif global_id.startswith('HGNC:'):
            complete_url = 'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=' + local_id
        elif global_id.startswith('NCBI_Gene'):
            complete_url = 'https://www.ncbi.nlm.nih.gov/gene/' + local_id
        elif global_id.startswith('UniProtKB'):
            complete_url = 'http://www.uniprot.org/uniprot/' + local_id
        elif global_id.startswith('ENSEMBL'):
            complete_url = 'http://www.ensembl.org/id/' + local_id
        elif global_id.startswith('RNAcentral'):
            complete_url = 'http://rnacentral.org/rna/' + local_id
        elif global_id.startswith('PMID'):
            complete_url = 'https://www.ncbi.nlm.nih.gov/pubmed/' + local_id
        elif global_id.startswith('SO:'):
            complete_url = 'http://www.sequenceontology.org/browser/current_svn/term/' + local_id
        elif global_id.startswith('DRSC'):
            complete_url = None
        elif global_id.startswith('PANTHER'):
            panther_url = 'http://pantherdb.org/treeViewer/treeViewer.jsp?book=' + local_id + '&species=agr'
            if primary_id.startswith('MGI'):
                split_primary = primary_id.split(':')[1]
                complete_url = panther_url + '&seq=MGI=MGI=' + split_primary
            elif primary_id.startswith('RGD'):
                split_primary = primary_id.split(':')[1]
                complete_url = panther_url + '&seq=RGD=' + split_primary
            elif primary_id.startswith('SGD'):
                complete_url = panther_url + '&seq=SGD=' + primary_id
            elif primary_id.startswith('FB'):
                split_primary = primary_id.split(':')[1]
                complete_url = panther_url + '&seq=FlyBase=' + split_primary
            elif primary_id.startswith('WB'):
                split_primary = primary_id.split(':')[1]
                complete_url = panther_url + '&seq=WormBase=' + split_primary
            elif primary_id.startswith('ZFIN'):
                split_primary = primary_id.split(':')[1]
                complete_url = panther_url + '&seq=ZFIN=' + split_primary

        return complete_url