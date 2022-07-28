def addActivity(tx, activityName):
    tx.run("MERGE (p:Activity {name: $activityName})", activityName=activityName)