from collections import defaultdict
from typing import Dict
from genedescriptions.descriptions_rules import Gene
from ontobio import Ontology, AssociationSetFactory
from ..transactions import Transaction

def get_ontology_from_loader_object(ontology_term_list) -> Ontology:
    ontology = Ontology()
    for term in ontology_term_list:
        if ontology.has_node(term["oid"]):
            # previously added as parent
            ontology.node(term["oid"])["label"] = term["name"]
        else:
            ontology.add_node(term["oid"], term["name"])
        if term["is_obsolete"] == "true":
            ontology.set_obsolete(term["oid"])
        ontology.node(term["oid"])["alt_ids"] = term["alt_ids"]
        for parent_id in term["isas"]:
            if not ontology.has_node(parent_id):
                ontology.add_node(parent_id, "")
            ontology.add_parent(term["oid"], parent_id, "subClassOf")
        for parent_id in term["partofs"]:
            if not ontology.has_node(parent_id):
                ontology.add_node(parent_id, "")
            ontology.add_parent(term["oid"], parent_id, "BFO:0000050")
    return ontology


def get_go_associations_from_loader_object(go_annotations, go_ontology):
    associations = []
    for gene_annotations in go_annotations:
        for annot in gene_annotations["annotations"]:
            if go_ontology.has_node(annot["go_id"]) and not go_ontology.is_obsolete(annot["go_id"]):
                associations.append({"source_line": "",
                                     "subject": {
                                         "id": gene_annotations["gene_id"],
                                         "label": "",
                                         "type": "",
                                         "fullname": "",
                                         "synonyms": [],
                                         "taxon": {"id": ""}

                                     },
                                     "object": {
                                         "id": annot["go_id"],
                                         "taxon": ""
                                     },
                                     "qualifiers": annot["qualifier"].lower().split("|"),
                                     "aspect": annot["aspect"],
                                     "relation": {"id": None},
                                     "negated": False,
                                     "evidence": {
                                         "type": annot["evidence_code"],
                                         "has_supporting_reference": "",
                                         "with_support_from": [],
                                         "provided_by": gene_annotations["dataProvider"],
                                         "date": None
                                     }
                                     })
    return AssociationSetFactory().create_from_assocs(assocs=associations, ontology=go_ontology)


def get_do_associations_from_loader_object(do_annotations, do_annotations_allele, do_ontology, data_provider):
    associations = []
    for gene_annotations in do_annotations:
        for annot in gene_annotations:
            if annot and "doId" in annot and do_ontology.has_node(annot["doId"]) and not \
                    do_ontology.is_obsolete(annot["doId"]):
                associations.append({"source_line": "",
                                     "subject": {
                                         "id": annot["primaryId"],
                                         "label": annot["diseaseObjectName"],
                                         "type": annot["diseaseObjectType"],
                                         "fullname": "",
                                         "synonyms": [],
                                         "taxon": {"id": ""}

                                     },
                                     "object": {
                                         "id": annot["doId"],
                                         "taxon": ""
                                     },
                                     "qualifiers":
                                         annot["qualifier"].lower().split("|") if annot["qualifier"] is not None
                                         else "",
                                     "aspect": "D",
                                     "relation": {"id": None},
                                     "negated": False,
                                     "evidence": {
                                         "type": annot["ecodes"][0],
                                         "has_supporting_reference": "",
                                         "with_support_from": [],
                                         "provided_by": annot["dataProvider"],
                                         "date": None
                                     }
                                     })
    for gene_annotations in do_annotations_allele:
        for annot in gene_annotations:
            inferred_genes = get_inferred_genes_for_allele(annot["primaryId"])
            if len(inferred_genes) == 1 and annot and "doId" in annot and do_ontology.has_node(annot["doId"]) \
                    and not do_ontology.is_obsolete(annot["doId"]):
                associations.append({"source_line": "",
                                     "subject": {
                                         "id": inferred_genes[0].id,
                                         "label": annot["diseaseObjectName"],
                                         "type": annot["diseaseObjectType"],
                                         "fullname": "",
                                         "synonyms": [],
                                         "taxon": {"id": ""}

                                     },
                                     "object": {
                                         "id": annot["doId"],
                                         "taxon": ""
                                     },
                                     "qualifiers":
                                         annot["qualifier"].lower().split("|") if annot["qualifier"] is not None
                                         else "",
                                     "aspect": "D",
                                     "relation": {"id": None},
                                     "negated": False,
                                     "evidence": {
                                         "type": annot["ecodes"][0],
                                         "has_supporting_reference": "",
                                         "with_support_from": [],
                                         "provided_by": annot["dataProvider"],
                                         "date": None
                                     }
                                     })
    # TODO: uncomment to include disease via orthology data
    # for annot in get_disease_annotations_via_orthology(data_provider=data_provider):
    #     associations.append({"source_line": "",
    #                          "subject": {
    #                              "id": annot["geneID"],
    #                              "label": None,
    #                              "type": None,
    #                              "fullname": "",
    #                              "synonyms": [],
    #                              "taxon": {"id": ""}
    #                          },
    #                          "object": {
    #                              "id": annot["doId"],
    #                              "taxon": ""
    #                          },
    #                          "qualifiers": "",
    #                          "aspect": "D",
    #                          "relation": {"id": None},
    #                          "negated": False,
    #                          "evidence": {
    #                              "type": "DVO",
    #                              "has_supporting_reference": "",
    #                              "with_support_from": [],
    #                              "provided_by": data_provider,
    #                              "date": None
    #                              }
    #                          })
    return AssociationSetFactory().create_from_assocs(assocs=associations, ontology=do_ontology)


def get_orthologs_from_loader_object(ortho_data, data_provider):
    orthologs = defaultdict(list)
    uuid_matched = defaultdict(int)
    for orth_lists in ortho_data:
        for method_matched in orth_lists[1]:
            uuid_matched[method_matched["uuid"]] += 1
        for orth in orth_lists[0]:
            if uuid_matched[orth["uuid"]] > 0 and orth['strictFilter'] is True:
                if orth['gene2AgrPrimaryId'].startswith('HGNC') and orth['gene1AgrPrimaryId'].startswith(data_provider):
                    orthologs[orth['gene1AgrPrimaryId']].append([orth['gene2AgrPrimaryId'], uuid_matched[orth["uuid"]]])
                elif orth['gene1AgrPrimaryId'].startswith('HGNC') and \
                        orth['gene2AgrPrimaryId'].startswith(data_provider):
                    orthologs[orth['gene2AgrPrimaryId']].append([orth['gene1AgrPrimaryId'], uuid_matched[orth["uuid"]]])
    orth_id_symbol_and_name = {gene[0]: [gene[1], gene[2]] for gene in get_gene_symbols_from_id_list(
        [ortholog[0] for orthologs in orthologs.values() for ortholog in orthologs])}
    return {gene_id: [[orth[0], *orth_id_symbol_and_name[orth[0]], orth[1]] for orth in orthologs if orth[0]
                      in orth_id_symbol_and_name] for gene_id, orthologs in orthologs.items()}


def query_db(query: str, parameters: Dict = None):
    return Transaction.run_single_parameter_query(query, parameters)

def get_gene_data_from_neo4j(data_provider):
    db_query = "match (g:Gene) where g.dataProvider = {dataProvider} return g.symbol, g.primaryKey"
    result_set = query_db(query=db_query, parameters={"dataProvider": data_provider})
    for result in result_set:
        yield Gene(result["g.primaryKey"], result["g.symbol"], False, False)


def get_inferred_genes_for_allele(allele_primary_key):
    db_query = "match (o:Allele)-[:IS_ALLELE_OF]-(g:Gene) where o.primaryKey = {allelePrimaryKey} return " \
               "g.symbol, g.primaryKey"
    result_set = query_db(query=db_query, parameters={"allelePrimaryKey": allele_primary_key})
    return [Gene(result["g.primaryKey"], result["g.symbol"], False, False) for result in result_set]


def get_gene_symbols_from_id_list(id_list):
    db_query = "match (g:Gene) " \
               "where g.primaryKey in {idList} " \
               "return g.primaryKey, g.symbol, g.name"
    result_set = query_db(query=db_query, parameters={"idList": id_list})
    return result_set


def get_best_orthologs_from_list(ortholog_list):
    max_num_methods = max([orth[3] for orth in ortholog_list])
    return [ortholog for ortholog in ortholog_list if ortholog[3] == max_num_methods]


def get_best_orthologs_for_genes_in_dict(gene_orthologs_dict):
    return {gene_id: get_best_orthologs_from_list(orthologs) for gene_id, orthologs in gene_orthologs_dict.items() if
            len(orthologs) > 0}


def get_disease_annotations_via_orthology(data_provider):
    db_query = """
    MATCH (disease:DOTerm)-[da:IS_IMPLICATED_IN|IS_MARKER_FOR]-(gene1:Gene)-[o:ORTHOLOGOUS]->(gene2:Gene)
    MATCH (ec:EvidenceCode)-[:EVIDENCE]-(dej:DiseaseEntityJoin)--(gene1:Gene)-[:FROM_SPECIES]->(species:Species)
    WHERE o.strictFilter 
    AND gene1.primaryKey contains "HGNC" 
    AND da.uuid = dej.primaryKey 
    AND NOT ec.primaryKey IN ["IEA", "ISS", "ISO"]
    AND gene2.dataProvider = {dataProvider}
    OPTIONAL MATCH (disease:DOTerm)-[da2:ASSOCIATION]-(gene2:Gene)-[ag:IS_ALLELE_OF]->(:Allele)-[da3:IS_IMPLICATED_IN|IS_MARKER_FOR]-(disease:DOTerm)
    WHERE da2 IS null  // filters relations that already exist
    AND da3 IS null // filter where allele already has disease association
    RETURN DISTINCT gene2.primaryKey AS geneID,
    species.primaryKey AS speciesID,
    type(da) AS relationType,
    disease.primaryKey AS doId"""

    return query_db(query=db_query, parameters={"dataProvider": data_provider})

