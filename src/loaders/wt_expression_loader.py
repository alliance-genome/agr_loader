from .transactions.wt_expression import WTExpressionTransaction


class ExpressionLoader(object):

    def __init__(self, graph):
        self.graph = graph

    def load_wt_expression_objects(self, data, species):
        tx = WTExpressionTransaction(self.graph)
        tx.wt_expression_object_tx(data, species)