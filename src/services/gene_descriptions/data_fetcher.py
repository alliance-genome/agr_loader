"""retrieve data for gene descriptions (GD) from neo4j and generate data structures and objects for GD"""
from collections import defaultdict
from typing import List, Tuple, Dict, Set
from genedescriptions.data_fetcher import DataFetcher, Gene, Ontology, GOTerm


class Neo4jGOOntology(Ontology):
    """ontology with the same properties and methods defined by goatools GODag

    only the properties used for gene descriptions are implemented
    """

    def __init__(self, root_terms: List[Tuple[str, str, List[str]]], node_children_dict, node_parents_dict, node_names,
                 alt_ids, obsolete_terms: Set[str]):
        super().__init__()
        self.node_children_dict = node_children_dict
        self.node_parents_dict = node_parents_dict
        self.node_depth = defaultdict(int)
        self.node_names = node_names
        self.alt_ids = alt_ids
        self.obsolete_terms = obsolete_terms
        for root_term in root_terms:
            self.calculate_all_depths_in_branch(root_term[0])

    def calculate_all_depths_in_branch(self, root_id: str, current_depth: int = 0):
        """calculate and set depth recursively for all terms in a branch

        :param root_id: the ID of the root term of the branch to process
        :type root_id: str
        :param current_depth: the current depth in the ontology
        :type current_depth: int
        """
        self.node_depth[root_id] = max(self.node_depth[root_id], current_depth)
        for child_id in self.node_children_dict[root_id]:
            self.calculate_all_depths_in_branch(root_id=child_id, current_depth=current_depth + 1)

    def query_term(self, term_id: str):
        """retrieve a term from its ID

        :param term_id: the ID of the term
        :type term_id: str
        :return: the term
        :rtype: GOTerm
        """
        if term_id not in self.node_names:
            if term_id in self.alt_ids:
                term_id = self.alt_ids[term_id]
            else:
                return None
        return GOTerm(name=self.node_names[term_id], depth=self.node_depth[term_id], node_id=term_id,
                      parents=self.node_parents_dict[term_id], children=self.node_children_dict[term_id], ontology=self,
                      is_obsolete=term_id in self.obsolete_terms)


class Neo4jDataFetcher(DataFetcher):
    """data fetcher for AGR neo4j database for a single species"""

    def __init__(self, go_terms_exclusion_list: List[str], go_terms_replacement_dict: Dict[str, str], db_graph,
                 data_provider, go_ontology=None):
        super().__init__(go_terms_exclusion_list=go_terms_exclusion_list,
                         go_terms_replacement_dict=go_terms_replacement_dict)
        self.db_graph = db_graph
        self.go_ontology = go_ontology
        self.data_provider = data_provider

    @staticmethod
    def query_db(db_graph, query: str, parameters: Dict = None):
        """query the neo4j db

        :param db_graph: a neo4j graph database
        :param query: a cypher
        :type query: str
        :param parameters: a dictionary of the parameters of the query
        :type parameters: Dict
        """
        with db_graph.session() as session:
            with session.begin_transaction() as tx:
                return_set = tx.run(query, parameters=parameters)
        return return_set

    def get_gene_data(self):
        """get all gene data from the fetcher, returning one gene per call

        while reading data for a gene, all related annotations are loaded from neo4j into memory

        :return: data for one gene per each call, including gene_id and gene_name
        :rtype: Gene
        """
        db_query = "match (g:Gene) where g.dataProvider = {dataProvider} return g.symbol, g.primaryKey"
        result_set = self.query_db(db_graph=self.db_graph, query=db_query,
                                   parameters={"dataProvider": self.data_provider})
        for result in result_set:
            yield Gene(result["g.primaryKey"], result["g.symbol"], False, False)

    def load_go_data(self, go_terms_list: List[Dict], go_annotations):
        """load GO ontology from the provided list of go terms, if not yet loaded"""
        if not self.go_ontology:

            go_terms_dict = {}
            for go_term in go_terms_list:
                go_terms_dict[go_term["id"]] = go_term

            # get root terms
            db_query = "match path=(child:GOTerm:Ontology)-[r:IS_A]->(parent:GOTerm:Ontology) " \
                       "where size((parent)-[:IS_A]->()) = 0 " \
                       "return distinct parent.primaryKey, parent.name"
            result_set = self.query_db(db_graph=self.db_graph, query=db_query)
            root_terms = []
            for result in result_set:
                root_terms.append((result["parent.primaryKey"], result["parent.name"],
                                   go_terms_dict[result["parent.primaryKey"]]["alt_ids"]))

            node_parents_dict = defaultdict(list)
            node_children_dict = defaultdict(list)
            node_names = {}
            alt_ids = {}
            obsolete_terms = set()
            for go_term in go_terms_list:
                for parent_go_id in set().union(go_term["isas"], go_term["partofs"]):
                    node_parents_dict[go_term["id"]].append(parent_go_id)
                    node_children_dict[parent_go_id].append(go_term["id"])
                node_names[go_term["id"]] = go_term["name"]
                for alt_id in go_term["alt_ids"]:
                    alt_ids[alt_id] = go_term["id"]
                if go_term["is_obsolete"] == "true":
                    obsolete_terms.add(go_term["id"])
            for root_term_id, root_term_name, root_term_alt_ids in root_terms:
                node_names[root_term_id] = root_term_name
                if root_term_alt_ids:
                    for alt_id in list(root_term_alt_ids):
                        alt_ids[alt_id] = root_term_id

            self.go_ontology = Neo4jGOOntology(root_terms=root_terms, node_children_dict=node_children_dict,
                                               node_parents_dict=node_parents_dict, node_names=node_names,
                                               alt_ids=alt_ids, obsolete_terms=obsolete_terms)

        for gene_annotations in go_annotations:
            for annot in gene_annotations["annotations"]:
                annotated_node = self.go_ontology.query_term(annot["go_id"])
                if annotated_node and not annotated_node.is_obsolete:
                    self.go_data[gene_annotations["gene_id"]].append({
                        "DB": gene_annotations["dataProvider"],
                        "DB_Object_ID": annot["go_id"],
                        "DB_Object_Symbol": None,
                        "Qualifier": annot["qualifier"].split("|"),
                        "GO_ID": annotated_node.id,
                        "DB:Reference": None,
                        "Evidence": annot["evidence_code"],
                        "With": "",
                        "Aspect": annot["aspect"],
                        "DB_Object_Name": None,
                        "Synonym": None,
                        "DB_Object_Type": None,
                        "Taxon_ID": None,
                        "Date": None,
                        "Assigned_By": None,
                        "Annotation_Extension": None,
                        "Gene_Product_Form_ID": None,
                        "GO_Name": annotated_node.name,
                        "Is_Obsolete": False
                    })
