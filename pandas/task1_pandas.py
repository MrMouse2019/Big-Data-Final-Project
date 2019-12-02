import pandas as pd
import numpy as np

import sys
import os
import json

from pandas.api.types import infer_dtype

from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_float_dtype
from pandas.api.types import is_integer_dtype
from pandas.api.types import is_int64_dtype
from pandas.api.types import is_signed_integer_dtype
from pandas.api.types import is_unsigned_integer_dtype

from pandas.api.types import is_datetime64_any_dtype
from pandas.api.types import is_datetime64_dtype
from pandas.api.types import is_datetime64_ns_dtype
from pandas.api.types import is_datetime64tz_dtype
from pandas.api.types import is_datetime64tz_dtype
from pandas.api.types import is_timedelta64_dtype
from pandas.api.types import is_timedelta64_ns_dtype

from pandas.api.types import is_period_dtype

from pandas.api.types import is_string_dtype


def getFilename(path):
	return path.split('/')[-1].split('.')[0]

dataset_path = "2k3g-r445.tsv.gz"
if len(sys.argv) >= 2:
	dataset_path = sys.argv[1]
	
filename = sys.argv[1]

df = pd.read_csv(filename, sep='\t', lineterminator='\n')

total_rows = len(df)
print ('Total Rows: ', total_rows)
print ('Total Columns: ', len(df.columns))

dataset_name = filename

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


for col in df.columns:
	column_output = {}

	column_output['column_name'] = col

	frequent_values = []
	top_5 = df[col].value_counts().nlargest(n=5)
	for key in top_5.index:
		frequent_values.append(key)

	#df[col].mean()

	number_empty_cells = len((np.where(pd.isnull(df[col])))[0])
	number_non_empty_cells = total_rows - number_empty_cells
	number_distinct_values = len(pd.unique(df[col]))

	data_type = []
	dict = {}

	data_types = pd.unique(list(map(type, df[col])))
	if len(data_types) > 1:
		print ('Multi-types Alert!!!!')
		print (col)
		print (data_types)

	if is_numeric_dtype(df[col].dtype):
		# 
		if is_float_dtype(df[col].dtype):
			dict["type"] = "REAL"
		else:
			dict["type"] = "INTEGER"
		dict["count"] = int(number_non_empty_cells)
		dict["max_value"] = float(df[col].max())
		dict["min_value"] = float(df[col].min())
		dict["mean"] = float(df[col].mean())
		dict["stddev"] = float(df[col].std())
	elif is_string_dtype(df[col].dtype):
		#
		largest_indices = df[col].str.len().nlargest(n=5).index
		long_five = []
		short_five = []
		for idx in largest_indices:
			long_five.append(df[col].iloc[idx])
		smallest_indices = df[col].str.len().nsmallest(n=5).index
		for idx in smallest_indices:
			short_five.append(df[col].iloc[idx])
		dict["type"] = "TEXT"
		dict["count"] = int(number_non_empty_cells)
		dict["shortest_values"] = short_five
		dict["longest_values"] = long_five
		dict["average_length"] = float(df[col].str.len().mean())
	else:
		print ('Other type: ', col)

	data_type.append(dict)

	column_output['number_non_empty_cells'] = number_non_empty_cells
	column_output['number_empty_cells'] = number_empty_cells
	column_output['number_distinct_values'] = number_distinct_values

	column_output['frequent_values'] = frequent_values
	column_output['data_type'] = data_type
	column_output['semantic_types'] = []
	output['columns'].append(column_output)

with open('task1_pandas/{}.json'.format(dataset_name), 'w') as outfile:
	json.dump(output, outfile, indent=4)


#df.info()
#df.describe(include='all')
#headerArray = df.head(0)
# list( map(type, df[col]))