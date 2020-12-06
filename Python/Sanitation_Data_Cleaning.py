import pandas as pd

# Read the file
data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-sanitation-code-complaints.csv",
                   low_memory=False)

data.rename(columns={'What is the Nature of this Code Violation?': 'code_nature'}, inplace=True)

# Output the number of rows
print("\nTotal rows: {0}".format(len(data)))
print(data.columns)

data = data.filter(['code_nature'], axis=1)

# Add the right id numbers
data.insert(0, 'incident_id', range(2780135, 2780135 + len(data)))

# DATA FORMAT
data['code_nature'] = data['code_nature'].fillna('').astype(str)

data.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/7/311-sanitation-code-complaints.csv", index=False, header=True)