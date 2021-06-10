import sys
import pandas as pd
from fastavro import reader

def csv_to_df(csv_file):
    df = pd.read_csv (csv_file)
    return df

def avro_to_df(avro_file):
    with open(avro_file, 'rb') as fp:
        # Configure Avro reader
        avro_reader = reader(fp)
        # Load records in memory
        records = [r for r in avro_reader]
        # Populate pandas.DataFrame with records
        df = pd.DataFrame.from_records(records)
    return df

def json_to_df(json_file):
	df = pd.read_json(json_file)
	return df

def convert_file(file_name):
    if 'csv' in file_name:
        return csv_to_df(file_name)
    elif 'avro' in file_name:
        return avro_to_df(file_name)
    elif 'json' in file_name:
        return json_to_df(file_name)
    else:
    	return ("Cannot Convert " + str(file_name))

##Get user input (we trust them to behave so aren't sanitizing)
file_list = input("Enter filenames separated by spaces")
file_arr = file_list.split()
print("Processing:")
print(' '.join(file_arr))

#Initialize first file as DataFrame
raw_data = convert_file(file_arr[0])

#Add other files
for file in file_arr:
	raw_data = pd.concat([raw_data, convert_file(file)])

##Dedupe
cleansed = raw_data.drop_duplicates()
cleansed = cleansed.reindex()

#Print final dataset
cleansed.to_csv('CleansedList.csv', index=False)

#print row count (using dataframe shape)
print('\n\n\nThe row count is ' + str(cleansed.shape[0]))

#print largest population using dataframe max
largest_pop = cleansed[cleansed['Population'] == cleansed['Population'].max()]['Name'].values[0]
print("\n\n\nLargest Population:  \n")
print(largest_pop)

#sum results after country code filter
print("\n\n\nPopulation of cities in BRAZIL: \n")
print(cleansed.loc[cleansed['CountryCode'] == 'BRA', 'Population'].sum())
print('\n\n\nAll done!')
