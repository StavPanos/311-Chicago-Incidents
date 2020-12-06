import pandas as pd

# Read the file
chunk = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-graffiti-removal.csv",
                   low_memory=False, chunksize=1000000)

data = pd.concat(chunk)

data.rename(columns={'What Type of Surface is the Graffiti on?': 'surface'}, inplace=True)
data.rename(columns={'Where is the Graffiti located?': 'located_at'}, inplace=True)

# Output the number of rows
print("\nTotal rows: {0}".format(len(data)))
print(data.columns)

data = data.filter(['surface', 'located_at'], axis=1)

# Add the right id numbers
data.insert(0, 'incident_id', range(847791, 847791 + len(data)))

# DATA FORMAT
data[['surface', 'located_at']] = data[['surface', 'located_at']].fillna('').astype(str)

data.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/4/311-graffiti-removal.csv", index=False, header=True)