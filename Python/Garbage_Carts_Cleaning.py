import pandas as pd

# Read the file
data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-garbage-carts.csv",
                   low_memory=False)

data.rename(columns=lambda x: x.strip().lower(), inplace=True)
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
data.rename(columns={'number_of_black_carts_delivered': 'carts_delivered'}, inplace=True)

# Output the number of rows
print("\nTotal rows: {0}".format(len(data)))
print(data.columns)

data = data.filter(['carts_delivered'], axis=1)

# Add the right id numbers
data.insert(0, 'incident_id', range(444000, 444000 + len(data)))

# DATA FORMAT
data['carts_delivered'] = data['carts_delivered'].fillna(0).astype('int')

data.loc[data['carts_delivered'] < 0, 'carts_delivered'] = 0

data.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/3/311-garbage-carts.csv", index=False, header=True)