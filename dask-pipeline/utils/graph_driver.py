class graph_driver():
    def __init__(self, uri_scheme='bolt', host='localhost', port='7687', username='neo4j', password='123456'):
        self.uri_scheme = uri_scheme
        self.host = host
        self.port = port

        self.username = username
        self.password = password

        self.connection_uri = "{uri_scheme}://{host}:{port}".format(uri_scheme=self.uri_scheme, host=self.host,
                                                                    port=self.port)
        self.auth = (self.username, self.password)
        self.driver = GraphDatabase.driver(self.connection_uri, auth=self.auth)

    def __del__(self):
        self._close_driver()

    def _close_driver(self):
        if self.driver:
            self.driver.close()

    def run_single_query(self, query):
        res = None
        with self.driver.session() as session:
            raw_res = session.run(query)
            res = self.format_raw_res(raw_res)
        return res

    def run_bulk_query(self, query_list):
        results = []
        with self.driver.session() as session:
            for query in tqdm(query_list):
                raw_res = session.run(query)
                res = self.format_raw_res(raw_res)
                results.append({'query': query, 'result': res})
        return results

    def reset_graph(self, db=None):
        return self.run_single_query("MATCH (n) DETACH DELETE n")

    def test_connection(self):
        return self.run_single_query("MATCH (n) RETURN COUNT(n) as nodes")

    @staticmethod
    def format_raw_res(raw_res):
        res = []
        for r in raw_res:
            res.append(r)
        return res