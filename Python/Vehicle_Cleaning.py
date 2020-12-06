import pandas as pd

# Read the file
data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-abandoned-vehicles.csv",
                   low_memory=False)

data.rename(columns=lambda x: x.strip().lower(), inplace=True)
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
data.rename(columns={'vehicle_make/model': 'make_model'}, inplace=True)
data.rename(columns={'vehicle_color': 'color'}, inplace=True)
data.rename(columns={'how_many_days_has_the_vehicle_been_reported_as_parked?': 'abandoded_days'}, inplace=True)

# Output the number of rows
print("\nTotal rows: {0}".format(len(data)))
print(data.columns)

data = data.filter(['license_plate', 'make_model', 'color', 'abandoded_days'], axis=1)

#Add the right id numbers
data.insert(0, 'incident_id', range(201266, 201266 + len(data)))


def func(column_name):
    count = 0
    for row in data[column_name]:
        row = str(row).strip()
        row = str(row).strip().replace(' ', '')
        symbols = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '=', '+', '?', ';', '<', '>', '.', ',','none','unknown', 'not ', 'no ', 'missing', 'xxxxxx', '-----',]
        if len(row) > 10 and any(tmp in row for tmp in symbols):
            row[0:50]
            print(row)
            count = count + 1
    print(count)

# DATA FORMAT
data['abandoded_days'] = data['abandoded_days'].fillna(0).astype('int')

data.loc[data['abandoded_days'] > 9223372036854775807, 'abandoded_days'] = 9999999

data.loc[data['abandoded_days'] < 0, 'abandoded_days'] = 0

data[['license_plate', 'make_model', 'color']] = data[['license_plate', 'make_model', 'color']].fillna('').astype(str)

data['color'] = data['color'].str.replace(' ','')

data.loc[data['color'].str.len() > 10, 'color'] = 'Multi-color'

data['license_plate'] = data['license_plate'].str.replace('  ','')

data['license_plate'] = data['license_plate'].str.strip().str.upper()

symbols = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '=', '+', '?', ';', '<', '>', '.', ',', '\'', '\"', 'none',
           'unknown', 'unkown', 'not ', 'no ', 'missing', 'xxxxxx', '-----']

for tmp in symbols:
    data.license_plate = data.license_plate.apply(lambda x: '' if tmp in str(x).lower() else x)

data['license_plate'] = data['license_plate'].str[:50]

func("license_plate")

data.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/1/311-vehicles.csv", index=False, header=True)