import pandas as pd
import numpy as np

import sys
import os
import json

from datetime import datetime


date_formats = [ 
"%Y %b %d %I %M %S %p",
"%Y %b %d %I %M %S%p",
"%Y %B %d %I %M %S %p",
"%Y %B %d %I %M %S%p",
"%Y %m %d %I %M %S %p",
"%Y %m %d %I %M %S%p",

"%Y %b %d %H %M %S",
"%Y %B %d %H %M %S",
"%Y %m %d %H %M %S",

"%B %d %Y %I %M %S%p",  
"%B %d %Y %I %M %S %p",
"%b %d %Y %I %M %S%p", 
"%b %d %Y %I %M %S %p",
"%m %d %Y %I %M %S%p",  
"%m %d %Y %I %M %S %p", 
"%B %d %Y %H %M %S",
"%b %d %Y %H %M %S"
"%m %d %Y %H %M %S",

"%B %d %Y %I %M%p",  
"%B %d %Y %I %M %p",
"%b %d %Y %I %M%p", 
"%b %d %Y %I %M %p",
"%m %d %Y %I %M%p",  
"%m %d %Y %I %M %p", 
"%B %d %Y %H %M",
"%b %d %Y %H %M"
"%m %d %Y %H %M",
"%b %d %Y", 
"%B %d %Y", 
"%m %d %Y",
"%Y %m %d", 
"%Y %b %d", 
"%Y %B %d",
"%H %M", 
"%I %M%p", 
"%I %M %p"
]
#2019-02-04T00:00:00.000? ==>  "%Y-%m-%dT%H%M%S.%f"

def getFilename(path):
	return path.split('/')[-1].split('.')[0]

# def is_int(val):
#     # if val is not a string:
#     if isinstance(val, int):
#         return True
#     # if val is a string
#     elif isinstance(val, str):
#         try:
#             int(val)
#             return True
#         except:
#             return False
#     else:
#         return False
#
# def is_real(val):
#     # if val is not a string
#     if isinstance(val, float):
#         return True
#     # if val is a string:
#     elif isinstance(val, str):
#         if is_int(val):
#             return False
#         try:
#             real_val = float(val)
#             if real_val == np.inf or real_val == -np.inf or real_val == np.nan:
#                 return False
#             return True
#         except:
#             return False
#     else:
#         return False

def is_int(val):
	try:
		np.int64(val)
		return True
	except:
		return False

def is_real(val):
	if is_int(val):
		return False
	try:
		real_val = float(val)
		if real_val == np.inf or real_val == -np.inf or real_val == np.nan:
			return False
		return True
	except:
		return False

def is_date(val):
	if is_int(val):
		return False
	if is_real(val):
		return False
	val = val.replace("-", " ")
	val = val.replace("/", " ")
	val = val.replace(",", " ")
	val = val.replace(":", " ")
	for date_format in date_formats:
		try:
			datetime_object = datetime.strptime(val, date_format)
			return date_format
		except:
			pass
	return False

def to_date(val):
	try:
		val = val.replace("-", " ")
		val = val.replace("/", " ")
		val = val.replace(",", " ")
		val = val.replace(":", " ")
		datetime_object = datetime.strptime(val, col_date_format)
		return datetime_object
	except:
		return np.nan

def is_text(val):
	if is_int(val):
		return False
	if is_real(val):
		return False
	return True

dataset_path = "test_data.tsv"
if len(sys.argv) >= 2:
	dataset_path = sys.argv[1]

dataset_name = getFilename(dataset_path)

df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n', nrows=0)
columns = list(df.columns)

#total_rows = len(df)
#print ('Total Rows: ', total_rows)
print ('Total Columns: ', len(columns))

json_path = 'task1_small/' + dataset_name + '.json'
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
col_id = 0

for col in columns:
	column_output = {}

	column_output['column_name'] = col
	col_id += 1
	try:
		print(col, time.ctime())
	except:
		print (col_id, time.ctime())

	df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n', usecols=[col], squeeze=True, dtype=object)
	if total_rows is None:
		total_rows = len(df)
		print ('Total Rows: ', total_rows)

	df = df.dropna()

	frequent_values = list(df.value_counts().nlargest(n=5).index)

	number_non_empty_cells = len(df)
	number_empty_cells = total_rows - number_non_empty_cells
	distinct = pd.unique(df)
	number_distinct_values = len(distinct)

	data_type = []

	if len(df):
		sample = df.iloc[0]
	else:
		try:
			print ('Empty column:', col)
		except:
			print ('Empty column:', col_id)
		column_output['number_non_empty_cells'] = number_non_empty_cells
		column_output['number_empty_cells'] = number_empty_cells
		column_output['number_distinct_values'] = number_distinct_values
		column_output['frequent_values'] = frequent_values
		column_output['data_type'] = data_type
		output['columns'].append(column_output)
		continue

	if (is_date(sample)):
		# DATE
		col_date_format = is_date(sample)
		#print (col_date_format)
		dict_date = {}
		#date_rows = lookup(df)
		distinct = pd.Series(distinct)
		distinct = distinct.apply(to_date)
		distinct = distinct.dropna()
		dict_date["type"] = "DATE/TIME"
		dict_date["count"] = number_non_empty_cells
		dict_date["max_value"] = str(distinct.max())
		dict_date["min_value"] = str(distinct.min())
		data_type.append(dict_date)
	else:
		# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.isdigit.html
		int_rows_idx = np.where(df.apply(is_int))[0]
		real_rows_idx = np.where(df.apply(is_real))[0]
		text_rows_idx = np.where(df.apply(is_text))[0]

		int_rows_cnt = len(int_rows_idx)
		real_rows_cnt = len(real_rows_idx)
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

		# TEXT
		if text_rows_cnt:
			dict_text = {}
			text_rows = df.iloc[text_rows_idx]
			text_len = text_rows.str.len()
			avg_length = float(text_len.mean())
			text_rows = pd.Series(pd.unique(text_rows))
			sort_len = text_rows.str.len().sort_values()
			smallest_idx = sort_len.head(5).index
			largest_idx = sort_len.tail(5).index
			long_five = []
			short_five = []
			for idx in largest_idx:
				str_value = text_rows.iloc[idx]
				#long_five.append([str_value, len(str_value)])
				long_five.append(str_value)
			long_five.reverse()

			for idx in smallest_idx:
				str_value = text_rows.iloc[idx]
				#short_five.append([str_value, len(str_value)])
				short_five.append(str_value)

			dict_text["type"] = "TEXT"
			dict_text["count"] = text_rows_cnt
			dict_text["shortest_values"] = short_five
			dict_text["longest_values"] = long_five
			dict_text["average_length"] = avg_length
			data_type.append(dict_text)

	column_output['number_non_empty_cells'] = number_non_empty_cells

	column_output['number_empty_cells'] = number_empty_cells
	column_output['number_distinct_values'] = number_distinct_values

	column_output['frequent_values'] = frequent_values
	column_output['data_type'] = data_type
	output['columns'].append(column_output)

with open('task1_small/{}.json'.format(dataset_name), 'w') as outfile:
	json.dump(output, outfile, indent=4)


#df.info()
#df.describe(include='all')
#headerArray = df.head(0)
# list( map(type, df[col]))