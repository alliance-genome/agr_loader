from etl import ETL

class BGIETL(ETL):

    def __init__(self, etl_config):
        self.config = etl_config.getBgiConfig()
        
        self.neo4j_query_template1 = "do stuff file file://%s"
        self.neo4j_query_template2 = ""
        self.neo4j_query_template3 = ""
        self.neo4j_query_template4 = ""

