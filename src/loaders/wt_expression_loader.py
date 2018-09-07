from .transactions.wt_expression import WTExpressionTransaction


class WTExpressionLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_wt_expression_objects(self, aoExpression, ccExpression, aoQualifier, aoSubstructure, aoSSQualifier, ccQualifier, aoccExpression, species):
        tx = WTExpressionTransaction(self.graph)
        tx.wt_expression_object_tx(aoExpression, ccExpression, aoQualifier, aoSubstructure, aoSSQualifier, ccQualifier, aoccExpression, species)