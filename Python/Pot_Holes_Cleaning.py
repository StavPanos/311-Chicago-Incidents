import pandas as pd

# Read the file
data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-pot-holes-reported.csv",
                   low_memory=False)

data.rename(columns={'NUMBER OF POTHOLES FILLED ON BLOCK': 'filled_pot_holes'}, inplace=True)

# Output the number of rows
print("\nTotal rows: {0}".format(len(data)))
print(data.columns)

data = data.filter(['filled_pot_holes'], axis=1)

# Add the right id numbers
data.insert(0, 'incident_id', range(1900470, 1900470 + len(data)))

# DATA FORMAT
data['filled_pot_holes'] = data['filled_pot_holes'].fillna(0).astype('int')

data['filled_pot_holes'] = data['filled_pot_holes'].astype(int)

data.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/5/311-pot-holes.csv", index=False, header=True)