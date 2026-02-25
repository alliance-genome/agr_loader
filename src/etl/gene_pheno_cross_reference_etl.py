"""Gene Pheno XREF ETL."""

import logging
import copy

from etl import ETL
from etl.helpers import ETLHelper, Neo4jHelper
from transactors import CSVTransactor, Neo4jTransactor


class GenePhenoCrossReferenceETL(ETL):
    """Gene Pheno XREF ETL."""

    logger = logging.getLogger(__name__)

    pheno_xref_query_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row
                MATCH (o:Gene {primaryKey: row.genePrimaryKey})
                """ + ETLHelper.get_cypher_xref_tuned_text() + """
            }
        IN TRANSACTIONS of %s ROWS
    """

    pheno_xref_relations_template = """
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
            CALL {
                WITH row
                MATCH (o:Gene {primaryKey: row.genePrimaryKey})
                MATCH (id:CrossReference {primaryKey: row.primaryKey})
                MERGE (o)-[gcr:CROSS_REFERENCE]->(id)
            }
        IN TRANSACTIONS of %s ROWS
    """

    gene_pheno_query_template = """
        MATCH (g:Gene)-[:HAS_PHENOTYPE]-(p:Phenotype)
        RETURN DISTINCT g.primaryKey AS genePrimaryKey, g.dataProvider AS dataProvider
    """

    def __init__(self, config):
        """Initialize object."""
        super().__init__()
        self.data_type_config = config

    def _load_and_process_data(self):
        """Load and process data."""
        commit_size = self.data_type_config.get_neo4j_commit_size()
        batch_size = self.data_type_config.get_generator_batch_size()

        # Prepare the generator that yields CSV rows
        generators = self.get_generators(batch_size)

        query_template_list = [
            [self.pheno_xref_query_template, "pheno_xref_data_.csv", commit_size],
            [self.pheno_xref_relations_template, "pheno_xref_relations_data_.csv", commit_size],
        ]

        # Build a list of (query, file) pairs to process
        query_and_file_list = self.process_query_params(query_template_list)

        # Write CSV files & execute them in Neo4j
        CSVTransactor.save_file_static(generators, query_and_file_list)
        Neo4jTransactor.execute_query_batch(query_and_file_list)

        self.error_messages()

    def _fetch_hgnc_to_rgd_mappings(self):
        """
        Returns a dictionary mapping:
          HGNC:#### -> RGD:####
        for genes with dataProvider = 'RGD' but primaryKey starts with 'HGNC:',
        which also have a CROSS_REFERENCE node whose primaryKey starts with 'RGD:'
        and crossRefType = 'generic_cross_reference'.
        """
        query = """
        MATCH (g:Gene {dataProvider: 'RGD'})
        WHERE g.primaryKey STARTS WITH 'HGNC:'
        MATCH (g)-[:CROSS_REFERENCE]->(cr:CrossReference)
        WHERE cr.primaryKey STARTS WITH 'RGD:'
          AND cr.crossRefType = 'generic_cross_reference'
        RETURN g.primaryKey AS hgncId, cr.primaryKey AS rgdId
        """
        mappings = {}
        with Neo4jHelper.run_single_query(query) as results:
            for record in results:
                hgnc_id = record["hgncId"]  # e.g., "HGNC:10402"
                rgd_id = record["rgdId"]    # e.g., "RGD:68661generic_cross_reference"
                if hgnc_id in mappings:
                    self.logger.warning(
                        f"Multiple RGD crossrefs found for HGNC gene {hgnc_id}. Already had '{mappings[hgnc_id]}', skipping '{rgd_id}'."
                    )
                else:
                    mappings[hgnc_id] = rgd_id
        return mappings

    def get_generators(self, batch_size):
        """Build CSV row dicts for Genes that have Phenotypes."""
        gene_pheno_data_list = []
        # Fetch the HGNC->RGD mappings once
        hgnc_to_rgd_map = self._fetch_hgnc_to_rgd_mappings()

        with Neo4jHelper.run_single_query(self.gene_pheno_query_template) as return_set:
            counter = 0
            for record in return_set:
                counter += 1
                global_cross_ref_id = record["genePrimaryKey"]  # e.g., "HGNC:10402" or "MGI:87853"
                data_provider = record["dataProvider"]
                id_prefix = global_cross_ref_id.split(":")[0]

                # MGI special case
                if data_provider == 'MGI':
                    page = 'gene/phenotypes_impc'
                    local_id = global_cross_ref_id.split(":")[1]
                    url = self.etlh.rdh2.return_url_from_key_value(id_prefix, local_id, page)
                    gene_pheno_xref = ETLHelper.get_xref_dict(
                        local_id=local_id,
                        prefix=id_prefix,
                        cross_ref_type=page,
                        page=page,
                        display_name="IMPC",
                        cross_ref_complete_url=url,
                        primary_id=global_cross_ref_id + page
                    )

                # RGD with HGNC prefix: reuse generic crossref data
                elif data_provider == 'RGD' and id_prefix == 'HGNC':
                    if global_cross_ref_id in hgnc_to_rgd_map:
                        rgd_id_full = hgnc_to_rgd_map[global_cross_ref_id]  # e.g., "RGD:68661generic_cross_reference"
                        rgd_prefix = rgd_id_full.split(":")[0]              # "RGD"
                        page = 'gene/phenotypes'
                        # Strip "generic_cross_reference" if present
                        clean_rgd_id = rgd_id_full
                        if clean_rgd_id.endswith("generic_cross_reference"):
                            clean_rgd_id = clean_rgd_id.replace("generic_cross_reference", "").rstrip()
                        rgd_number = clean_rgd_id.split(":")[1]             # e.g., "68661"
                        url = self.etlh.rdh2.return_url_from_key_value(rgd_prefix, rgd_number, page)
                        final_primary_key = clean_rgd_id + page  # e.g., "RGD:68661gene/phenotypes"
                        gene_pheno_xref = ETLHelper.get_xref_dict(
                            local_id=rgd_number,
                            prefix=rgd_prefix,
                            cross_ref_type=page,
                            page=page,
                            display_name=rgd_prefix,
                            cross_ref_complete_url=url,
                            primary_id=final_primary_key
                        )
                    else:
                        self.logger.warning(
                            f"No RGD crossreference found for Gene '{global_cross_ref_id}' even though it's labeled RGD + HGNC."
                        )
                        continue

                # Default logic
                else:
                    page = 'gene/phenotypes'
                    local_id = global_cross_ref_id.split(":")[1]
                    url = self.etlh.rdh2.return_url_from_key_value(id_prefix, local_id, page)
                    gene_pheno_xref = ETLHelper.get_xref_dict(
                        local_id=local_id,
                        prefix=id_prefix,
                        cross_ref_type=page,
                        page=page,
                        display_name=id_prefix,
                        cross_ref_complete_url=url,
                        primary_id=global_cross_ref_id + page
                    )

                # Attach the gene's own primary key for later MERGE
                gene_pheno_xref["genePrimaryKey"] = global_cross_ref_id
                gene_pheno_data_list.append(gene_pheno_xref)

                if counter == batch_size:
                    yield [gene_pheno_data_list, gene_pheno_data_list]
                    gene_pheno_data_list = []
                    counter = 0

            if counter > 0:
                yield [gene_pheno_data_list, gene_pheno_data_list]
