import pandas as pd

# Read the file
# data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-tree-trims.csv",
                 # low_memory=False)

data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-tree-trims.csv",
                   low_memory=False)

# data.rename(columns={'If Yes, where is the debris located?': 'located_at'}, inplace=True)
data.rename(columns={'Location of Trees': 'located_at'}, inplace=True)


# Output the number of rows
print("\nTotal rows: {0}".format(len(data)))
print(data.columns)

data = data.filter(['located_at'], axis=1)

# Add the right id numbers
data.insert(0, 'incident_id', range(4102833, 4102833 + len(data)))

# DATA FORMAT
data['located_at'] = data['located_at'].fillna('').astype(str)

data.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/11/311-tree-trims.csv", index=False, header=True)