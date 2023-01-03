class Dask_DFG():

    def getQueries(activities):
        listOfQueries = []
        queryTemplate = """
            MERGE (a:Activity {{name: '{activity}'}})
            MERGE (s:Activity {{name: '{successor}'}})
            MERGE (a)-[r:PRODUCES {{cost: '{cost}'}}]->(s)
        """
        for index, record in activities.iterrows():
            template = queryTemplate.format(activity=record['activityNameEN'], successor=record['successor'], cost=1)
            listOfQueries.append(template)
        return listOfQueries

    def saveActivities(activities):
        read_queries_start_time = time.time()
        activitiesQueries = getQueries(activities)
        neo4jConnection = graph_driver(uri_scheme="neo4j", host="neo4j", password="123456")
        result = neo4jConnection.run_bulk_query(activitiesQueries)
        read_queries_time = time.time() - read_queries_start_time
        print("----Finshed saving nodes: in {time}".format(time=str(read_queries_time)))