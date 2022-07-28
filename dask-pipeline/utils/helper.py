# this function is responsible for setting the parent(predecessor) and child(successor) for each element of a grouped data
def setLinks(row):
    row['predecessor'] = row['activityNameEN'].shift(1); #lag(1)
    row['successor'] = row['activityNameEN'].shift(-1); #lead(1)
    return row;