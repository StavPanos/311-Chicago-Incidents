import pandas as pd

# Read the file
data = pd.read_csv("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/csv/311-service-requests-abandoned-vehicles.csv", low_memory=False, na_values='-')

data.fillna(0, inplace=True)

data.rename(columns=lambda x: x.strip(), inplace=True)
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

# Output the number of rows
print("\nTotal rows: {0}".format(len(data)))

print(data.columns)

def count_columns(column_name):
    count = 0
    # See which headers are available
    for row in data[column_name]:
        row = int(row)
        if int(float(row)) > 32767:
            print(row)
            count = count + 1
    print("------------------------")
    print(count)

count_columns("How_Many_Days_Has_the_Vehicle_Been_Reported_as_Parked?")

print ("------------------------\n")

abnormal_license_plate_number = data[data.License_Plate.str.strip().str.find("#") > -1]

#print(abnormal_license_plate_number.License_Plate.to_string(index=False))

#print("! rows: {0}".format(len(abnormal_license_plate_number)))

abnormal_license_plate_number = data[data.License_Plate.str.strip().str.lower() == ""]

print("Empty rows: {0}".format(len(abnormal_license_plate_number)))

abnormal_license_plate_number = data[data.License_Plate.str.strip().str.lower() == "none"]

print("NONE rows: {0}".format(len(abnormal_license_plate_number)))

abnormal_license_plate_number = data[data.License_Plate.str.strip().str.lower() == "unknown"]

print("UNKNOWN rows: {0}".format(len(abnormal_license_plate_number)))

abnormal_license_plate_number = data[data.License_Plate.str.strip().str.lower() == "n/a"]

print("N/A rows: {0}".format(len(abnormal_license_plate_number)))

#write change to new csv
#pd.to_csv ("C:/Users/stavropoulosp/Desktop/M149/311-Incidents/cleaned_csv/311-service-requests-abandoned-vehicles.csv", index = False, header=True)



