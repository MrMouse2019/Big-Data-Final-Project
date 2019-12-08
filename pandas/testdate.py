import pandas as pd
import numpy as np

import sys
import os
import json

from dateutil.parser import *


def getFilename(path):
	return path.split('/')[-1].split('.')[0]

def is_int(val):
	return False

def is_real(val):
	return False

def is_date(val):
	return True

def is_text(val):
	return False


def classify(val):
	if is_int(val):
		return 0
	if is_int(val):
		return 1
	if is_int(val):
		return 2
	return 3

def lookup(s):
    """
    This is an extremely fast approach to datetime parsing.
    For large data, the same dates are often repeated. Rather than
    re-parse these, we store all unique dates, parse them, and
    use a lookup to convert all dates.
    """
    dates = {date:pd.to_datetime(date) for date in s.unique()}
    return s.map(dates)


dataset_path = "2k3g-r445.tsv.gz"
if len(sys.argv) >= 2:
	dataset_path = sys.argv[1]

dataset_name = getFilename(dataset_path)

df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n', nrows=0)
columns = list(df.columns)

#total_rows = len(df)
#print ('Total Rows: ', total_rows)
print ('Total Columns: ', len(columns))

json_path = 'task1_pandas/' + dataset_name + '.json'
if os.path.exists(json_path):
	print("Dataset: {} has been processed!".format(dataset_name))
	exit()

output = {}
output['dataset_name'] = dataset_name
# columns
output['columns'] = []
# key_column_candidates
output['key_column_candidates'] = []

import time

total_rows = None

# col = 'tpep_pickup_datetime'
col = 'FEE DATE'

column_output = {}

column_output['column_name'] = col

print (col, time.ctime())

df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n', usecols=[col], squeeze=True)
if total_rows is None:
	total_rows = len(df)
	print ('Total Rows: ', total_rows)



frequent_values = list(df.value_counts().nlargest(n=5).index)

number_empty_cells = len((np.where(pd.isnull(df)))[0])
number_non_empty_cells = total_rows - number_empty_cells
number_distinct_values = len(pd.unique(df))

data_type = []

# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.isdigit.html
int_rows_idx = np.where(df.apply(is_int))[0]
real_rows_idx = np.where(df.apply(is_real))[0]
if 'date' in col.lower() or 'time' in col.lower():
	date_rows_idx = (np.where(pd.notna(df)))[0]
	text_rows_idx = []
else:
	date_rows_idx = []
	text_rows_idx = np.where(df.apply(is_text))[0]

int_rows_cnt = len(int_rows_idx)
real_rows_cnt = len(real_rows_idx)
date_rows_cnt = len(date_rows_idx)
text_rows_cnt = len(text_rows_idx)

# INTEGER
if int_rows_cnt:
	int_rows = df.iloc[int_rows_idx].astype('int64')
	dict_int = {}
	dict_int["type"] = "INTEGER"
	dict_int["count"] = int_rows_cnt
	dict_int["max_value"] = int(int_rows.max())
	dict_int["min_value"] = int(int_rows.min())
	dict_int["mean"] = float(int_rows.mean())
	dict_int["stddev"] = float(int_rows.std(ddof=0))
	data_type.append(dict_int)

# REAL
if real_rows_cnt:
	dict_real = {}
	real_rows = df.iloc[real_rows_idx].astype('float64')
	dict_real["type"] = "REAL"
	dict_real["count"] = real_rows_cnt
	dict_real["max_value"] = float(real_rows.max())
	dict_real["min_value"] = float(real_rows.min())
	dict_real["mean"] = float(real_rows.mean())
	dict_real["stddev"] = float(real_rows.std(ddof=0))
	data_type.append(dict_real)

column_output['number_non_empty_cells'] = number_non_empty_cells
column_output['number_empty_cells'] = number_empty_cells
column_output['number_distinct_values'] = number_distinct_values

column_output['frequent_values'] = frequent_values
column_output['data_type'] = data_type
output['columns'].append(column_output)

# DATE/TIME
if date_rows_cnt:
	dict_date = {}
	print ('IS DATE!!!')
	#date_rows = lookup(df)
	dict_date["type"] = "DATE/TIME"
	dict_date["count"] = date_rows_cnt
	dict_date["max_value"] = str(date_rows.max())
	dict_date["min_value"] = str(date_rows.min())
	data_type.append(dict_date)

# TEXT
if text_rows_cnt:
	dict_text = {}
	text_rows = df.iloc[text_rows_idx]
	dict_text["average_length"] = float(text_rows.str.len().mean())
	text_rows = pd.Series(pd.unique(text_rows))

	largest_idx = text_rows.str.len().nlargest(n=5).index
	smallest_idx = text_rows.str.len().nsmallest(n=5).index
	long_five = []
	short_five = []
	for idx in largest_idx:
		str_value = text_rows.iloc[idx]
		long_five.append([str_value, len(str_value)])
	smallest = text_rows.str.len().value_counts().nsmallest(n=5)
	for idx in smallest_idx:
		str_value = text_rows.iloc[idx]
		short_five.append([str_value, len(str_value)])

	dict_text["type"] = "TEXT"
	dict_text["count"] = text_rows_cnt
	dict_text["shortest_values"] = short_five
	dict_text["longest_values"] = long_five
	data_type.append(dict_text)

with open('task1_pandas/{}.json'.format(dataset_name), 'w') as outfile:
	json.dump(output, outfile, indent=4)


#df.info()
#df.describe(include='all')
#headerArray = df.head(0)
# list( map(type, df[col]))