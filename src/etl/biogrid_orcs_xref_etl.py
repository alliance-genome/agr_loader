"""BIOGRID ORCS XREF ETL."""

import json
import logging
import urllib
import xmltodict
import glob, csv, os
from pathlib import Path

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor


class BiogridOrcsXrefETL(ETL):
    """BIOGRID ORCS XREF ETL."""

    logger = logging.getLogger(__name__)

    biogrid_orcs_xref_query_template = """
        USING PERIODIC COMMIT %s
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row

        MATCH (o:Gene) where o.primaryKey = row.genePrimaryKey
        """ + ETLHelper.get_cypher_xref_text()

    gene_crossref_query_template = """
                   MATCH (g:Gene)-[crr:CROSS_REFERENCE]-(cr:CrossReference)
                   WHERE cr.globalCrossRefId IN {parameter}
                   RETURN g.primaryKey, g.modLocalId, cr.name, cr.globalCrossRefId"""

    def __init__(self, config):
        """Initialise object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):

        entrez_ids = self.populate_entrez_ids_from_files()

        commit_size = self.data_type_config.get_neo4j_commit_size()
        # batch_size = self.data_type_config.get_generator_batch_size()
        batch_size = 100000

        generators = self.get_generators(batch_size, entrez_ids)

        query_template_list = [
            [self.biogrid_orcs_xref_query_template, commit_size, "biogrid_orcs_xref_data.csv"],
        ]

        query_and_file_list = self.process_query_params(query_template_list)
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)
        self.error_messages()

    def populate_entrez_ids_from_files(self):
        """Files are species-independent, so can generate entrez_ids just once instead of in get_generators"""
        entrez_ids = []

        # hide files elsewhere, since there's nearly 1000 of them, taking up 1.6Gb
        os.system('mkdir tmp/biogrid-orcs')
        os.system('mv -f tmp/BIOGRID-ORCS-SCREEN*.screen.tab.txt tmp/biogrid-orcs')
        for filename in glob.glob("tmp/biogrid-orcs/BIOGRID-ORCS-SCREEN*.screen.tab.txt"):
            self.logger.debug("processing %s", filename)
            with open(filename, 'r', encoding='utf-8') as filename_in:
                csv_reader = csv.reader(filename_in, delimiter='\t', quoting=csv.QUOTE_NONE)
                next(csv_reader, None)	# Skip the headers
                for row in csv_reader:
                    if row[2] == 'ENTREZ_GENE':
                        entrez_ids.append("NCBI_Gene:" + row[1])
        return entrez_ids

    def get_generators(self, batch_size, entrez_ids):
        """Get Generators."""

        biogrid_orcs_data_list = []
        return_set = Neo4jHelper.run_single_parameter_query(self.gene_crossref_query_template,
                                                            entrez_ids)

        for record in return_set:
            gene_primary_key = record["g.primaryKey"]
            mod_local_id = record["g.modLocalId"]
            global_cross_ref_id = record["cr.globalCrossRefId"]
            url = self.etlh.rdh2.return_url_from_key_value('NCBI_Gene', global_cross_ref_id.split(":")[1], 'biogrid/orcs')
            biogrid_orcs_xref = ETLHelper.get_xref_dict(global_cross_ref_id.split(":")[1],
                                               "NCBI_Gene",
                                               "gene/biogrid_orcs",
                                               "gene/biogrid_orcs",
                                               "BioGRID CRISPR Screen Cell Line Phenotypes",
                                               url,
                                               global_cross_ref_id+"gene/biogrid_orcs")

            biogrid_orcs_xref["genePrimaryKey"] = gene_primary_key
            biogrid_orcs_xref["modLocalId"] = mod_local_id

            biogrid_orcs_data_list.append(biogrid_orcs_xref)

        yield [biogrid_orcs_data_list]

