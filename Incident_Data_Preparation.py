import pandas as pd

#incident sequence starting number
sequence = 4102833
incident_type = "tree-trims"
#service request type codification
service_request_type = "11"
#new folder name
folder_name = service_request_type

# Read the file
data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-"+incident_type+".csv",
                   low_memory=False)

# Rename columns
data.rename(columns=lambda x: x.strip().lower(), inplace=True)
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
data.rename(columns={'type_of_service_request': 'service_request_type'}, inplace=True)

data.rename(columns={'zip': 'zip_code'}, inplace=True)

# Output the number of rows and column names
print("\nTotal rows: {0}".format(len(data)))
print(data.columns, "\n")


def extra_info():
    data.rename(columns={'historical_wards_2003-2015': 'historical_wards'}, inplace=True)

    ext_info = data.filter(['historical_wards', 'zip_codes', 'community_areas', 'census_tracts', 'wards'], axis=1)

    ext_info[['historical_wards', 'zip_codes', 'community_areas', 'census_tracts', 'wards']] \
        = ext_info[['historical_wards', 'zip_codes', 'community_areas', 'census_tracts', 'wards']].fillna(0).astype(
        'int')

    ext_info.insert(0, 'incident_id', range(sequence, sequence + len(ext_info)))

    ext_info.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/"+folder_name+"/"+incident_type+"_ext_info.csv",
                    index=False, header=True)


def act():
    activity = data.filter(['current_activity', 'most_recent_action'], axis=1)

    activity[['current_activity', 'most_recent_action']] = activity[['current_activity', 'most_recent_action']].fillna(
        '').astype(str)

    data['current_activity'] = data['current_activity'].str.strip().str.upper()

    data['most_recent_action'] = data['most_recent_action'].str.strip().str.upper()

    activity.insert(0, 'incident_id', range(sequence, sequence + len(activity)))

    activity.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/"+folder_name+"/"+incident_type+"_activity.csv",
                    index=False, header=True)


def ssa():
    ssa = data.filter(['ssa'], axis=1)

    ssa[['ssa']] = ssa[['ssa']].fillna(0).astype('int')

    ssa.insert(0, 'incident_id', range(sequence, sequence + len(ssa)))

    ssa.to_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/"+folder_name+"/"+incident_type+"_ssa.csv",
               index=False, header=True)


extra_info()
#act()
#ssa()

data = data.filter(["service_request_number", "service_request_type", "creation_date", "completion_date", "status",
                    "street_address", "zip_code", "x_coordinate", "y_coordinate", "ward", "police_district",
                    "community_area", "latitude", "longitude", "location"], axis=1)

# SERVICE REQUEST TYPE CODE
data['service_request_type'] = service_request_type

# DATA FORMAT
data[['zip_code', 'ward', 'police_district', 'community_area']] \
    = data[['zip_code', 'ward', 'police_district', 'community_area']].fillna(0).astype('int')

data[['zip_code', 'police_district', 'community_area', 'x_coordinate', 'y_coordinate', 'latitude', 'longitude']] \
    = data[
    ['zip_code', 'police_district', 'community_area', 'x_coordinate', 'y_coordinate', 'latitude', 'longitude']].fillna(
    '').astype(str)

print(data.dtypes)

data['creation_date'] = data.creation_date.str.replace('T', ' ')

data.completion_date = data.completion_date.fillna('')
data['completion_date'] = data.completion_date.str.replace('T', ' ')

data['creation_date'] = data['creation_date'].str[:19]
data['completion_date'] = data['completion_date'].str[:19]

# Write to new csv
data.to_csv(
    "C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/"+folder_name+"/311-service-requests-"+incident_type+".csv",
    index=False, header=True)