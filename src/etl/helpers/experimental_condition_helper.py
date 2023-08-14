"""Experimental condition Helper"""

import logging


class ExperimentalConditionHelper():
    """Experimental condition Helper"""

    logger = logging.getLogger(__name__)

    def __init__(self, entity_join_label):
        self.record_cond_relations = []

        self.cond_relations = []
        self.cond_nodes = dict()

        self.execute_exp_condition_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (zeco:ZECOTerm {primaryKey:row.conditionClassId})

                MERGE (ec:ExperimentalCondition {primaryKey:row.ecUniqueKey})
                    ON CREATE SET ec.conditionClassId     = row.conditionClassId,
                                ec.conditionId          = row.conditionId,
                                ec.anatomicalOntologyId = row.anatomicalOntologyId,
                                ec.chemicalOntologyId   = row.chemicalOntologyId,
                                ec.geneOntologyId       = row.geneOntologyId,
                                ec.NCBITaxonID          = row.NCBITaxonID,
                                ec.conditionStatement   = row.conditionStatement

                MERGE (ec)-[:ASSOCIATION]-(zeco)

                WITH ec, row.chemicalOntologyId AS chemicalOntologyId
                MATCH (chebi:CHEBITerm {primaryKey: chemicalOntologyId})
                MERGE (ec)-[:ASSOCIATION]-(chebi)
            }
        IN TRANSACTIONS of %s ROWS"""

        self.execute_exp_condition_relations_query_template = """
        LOAD CSV WITH HEADERS FROM \'file:///%s\' AS row
            CALL {
                WITH row

                MATCH (dfa:Association:"""+entity_join_label+""" {primaryKey:row.entityUniqueKey})
                MATCH (ec:ExperimentalCondition {primaryKey:row.ecUniqueKey})

                CALL apoc.merge.relationship(dfa, row.relationshipType, null, {conditionQuantity: row.conditionQuantity}, ec) yield rel
                REMOVE rel.noOp
            }
        IN TRANSACTIONS of %s ROWS"""

    def conditionrelations_process(self, entity_record) -> str:
        """Condition relations (JSON) processing.
            Returns the concatenated ecUniqueKey string of the result."""

        self.record_cond_relations = []

        if 'conditionRelations' not in entity_record:
            # No condition relation annotation to parse
            return self.get_concat_ec_key()

        for relation in entity_record['conditionRelations']:
            for condition in relation['conditions']:
                # Store unique conditions
                # Unique condition key: conditionStatement + conditionClassId + conditionId
                #     + (anatomicalOntologyId | chemicalOntologyId | geneOntologyId | NCBITaxonID)
                unique_key = str( condition.get('conditionStatement') or '' ) \
                              + condition.get('conditionClassId') \
                              + str( condition.get('conditionId') or '' ) \
                              + str( condition.get('anatomicalOntologyId') or '' ) \
                              + str( condition.get('chemicalOntologyId') or '' ) \
                              + str( condition.get('geneOntologyId') or '' ) \
                              + str( condition.get('NCBITaxonID') or '' )

                if unique_key not in self.cond_nodes:
                    condition_dataset = {
                        "ecUniqueKey": unique_key,
                        "conditionClassId":     condition.get('conditionClassId'),
                        "conditionId":          condition.get('conditionId'),
                        'anatomicalOntologyId': condition.get('anatomicalOntologyId'),
                        'chemicalOntologyId':   condition.get('chemicalOntologyId'),
                        'geneOntologyId':       condition.get('geneOntologyId'),
                        'NCBITaxonID':          condition.get('NCBITaxonID'),
                        'conditionStatement':   condition.get('conditionStatement')
                    }

                    self.cond_nodes[unique_key] = condition_dataset

                # Store the relation between condition and entity_record
                relation_dataset = {
                    'ecUniqueKey': unique_key,
                    'relationshipType': relation.get('conditionRelationType').upper(),
                    'conditionQuantity': condition.get('conditionQuantity'),
                    # entity's UniqueKey to be appended after fn completion, as the combination
                    #  of all conditions defines a unique object (and thus its UniqueKey)
                }

                self.record_cond_relations.append(relation_dataset)

        return self.get_concat_ec_key()


    def get_concat_ec_key(self) -> str:
        """Return the concatenated ecUniqueKey string (sorted, to ensure consistent result)"""
        
        concat_ec_key = ""
        for cond_rel in sorted(self.record_cond_relations, key=lambda rel: rel["ecUniqueKey"]):
            concat_ec_key += cond_rel["ecUniqueKey"]

        return concat_ec_key


    def complete_record_cond_rels(self, entity_unique_key) -> None:
        """Complete the current record's experimental condition relations
            by adding the `entity_unique_key` to every relation."""

        for cond_rel in self.record_cond_relations:
            cond_rel["entityUniqueKey"] = entity_unique_key


    def commit_record_cond_rels(self) -> None:
        """Commit the current record's experimental condition relations (once completely annotated)"""

        self.cond_relations.extend(self.record_cond_relations)
        self.record_cond_relations = []


    def complete_and_commit_record_cond_rels(self, entity_unique_key) -> None:
        """Complete and commit record's experimental condition relations
            (see commit_record_cond_rels and complete_record_cond_rels)."""

        self.complete_record_cond_rels(entity_unique_key)
        self.commit_record_cond_rels()


    def get_cond_rels(self) -> list:
        """Return all stored experimental condition relations."""

        return self.cond_relations

    def get_cond_nodes(self) -> list:
        """Return all stored experimental condition nodes."""

        return self.cond_nodes.values()


    def reset(self) -> None:
        """Reset all stored experimental condition results."""

        self.record_cond_relations = []
        self.cond_relations = []
        self.cond_nodes = dict()