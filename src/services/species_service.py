class SpeciesService(object):

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
        elif taxon_id in ("taxon:559292"):
            return "Saccharomyces cerevisiae"
        elif taxon_id in ("NCBITaxon:7227"):
            return "Drosophila melanogaster"
        elif taxon_id in ("NCBITaxon:9606"):
            return "Homo sapiens"
        else:
            return None
