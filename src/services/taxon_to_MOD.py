def get_MOD_from_taxon(taxon_id):

    taxon_mod_dict = {
        '7955' : 'ZFIN',
        '6239' : 'WB',
        '10090' : 'MGI',
        '10116' : 'RGD',
        '559292' : 'SGD',
        '7227' : 'FB',
        '9606' : 'Human'
    }

    return taxon_mod_dict[taxon_id]