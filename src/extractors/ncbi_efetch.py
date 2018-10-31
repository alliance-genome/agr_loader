import urllib

class NCBIEfetch(object):

    def __init__(self, species, retmax, term, db, urlPrefix):
        self.speciesString = urllib.parse.quote_plus(species)
        self.retmax = retmax
        self.term = term
        self.db = db
        self.urlPrefix = urlPrefix

    def get_efetch_query_url(self):

        return self.urlPrefix + "term="+self.term + "[filter]" + "+AND+" + self.speciesString + "[Organism]" + "&retmax=" + self.retmax + "&db=" + self.db
