def addActivity(tx, activityName):
    tx.run("MERGE (p:Activity {name: $activityName})", activityName=activityName)

def linkActivity(tx, successor, predecessor, cost):
    tx.run("""
        MERGE (p:Activity {name: $successor})
        MERGE (c:Activity {name: $predecessor})
        MERGE (p)-[r:PRODUCES {cost: $cost}]->(c)
    """, successor=successor, predecessor=predecessor, cost=cost)