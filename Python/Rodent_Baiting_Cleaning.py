import pandas as pd

# Read the file
data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-rodent-baiting.csv",
                   low_memory=False)

data.rename(columns=lambda x: x.strip().lower(), inplace=True)
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
data.rename(columns={'number_of_premises_baited': 'premises_baited'}, inplace=True)
data.rename(columns={'number_of_premises_with_garbage': 'with_garbage'}, inplace=True)
data.rename(columns={'number_of_premises_with_rats': 'with_rats'}, inplace=True)

# Output the number of rows
print("\nTotal rows: {0}".format(len(data)))
print(data.columns)

data = data.filter(['premises_baited', 'with_garbage', 'with_rats'], axis=1)

# Add the right id numbers
data.insert(0, 'incident_id', range(2460948, 2460948 + len(data)))

# DATA FORMAT
data['premises_baited'] = data['premises_baited'].fillna(0).astype('int')
data['with_garbage'] = data['with_garbage'].fillna(0).astype('int')
data['with_rats'] = data['with_rats'].fillna(0).astype('int')

data.loc[data['premises_baited'] < 0, 'premises_baited'] = 0
data.loc[data['with_garbage'] < 0, 'with_garbage'] = 0
data.loc[data['with_rats'] < 0, 'with_rats'] = 0

data.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/6/311-rodent-baiting..csv", index=False, header=True)