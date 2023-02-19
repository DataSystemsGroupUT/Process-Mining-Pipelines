from .graph_driver import graph_driver
from pm4py import discover_dfg_typed as dfg_discovery
class Dask_DFG():

    def getDFGQueries(self, dfg):
        listOfQueries = []
        queryTemplate = """
            MERGE (p:Activity {{name: '{parent}'}})
            MERGE (c:Activity {{name: '{child}'}})
            MERGE (p)-[r:PRODUCES]->(c)
            ON CREATE SET r.frequency={frequency}
            ON MATCH SET r.frequency=r.frequency+{frequency}
        """
        for parent, child in dfg:
            frequency = dfg[(parent, child)]
            template = queryTemplate.format(parent=parent, child=child, frequency=frequency)
            listOfQueries.append(template)
        return listOfQueries

    def saveDFG(self, dfg):
        dfgResult = dfg_discovery(dfg)
        dfgQuery = self.getDFGQueries(dfgResult.graph)
        neo4jConnection = graph_driver(uri_scheme="neo4j", host="neo4j", password="123456")
        result = neo4jConnection.run_bulk_query(dfgQuery)
        return dfgResult