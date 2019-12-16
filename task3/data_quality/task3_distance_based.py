# Use 3 sigma method to detect possible value outliers (values that are too big or too small)

import pandas as pd
import numpy as np

import sys

import spark

def is_out(val):
	if val > float(mean + 3 * std):
		return True
	if val < float(mean - 3 * std):
		return True
	return False

dataset_path = sys.argv[1]

df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n')

col_name = sys.argv[2]

col = pd.DataFrame(df[col_name].astype('float64'))

min = col.min()
max = col.max()
std = col.std()
mean = col.mean()

col = df[col_name].astype('float64')

out_idx = np.where(col.apply(is_out))[0]

out_rows = col.iloc[out_idx]

print ('Possible Outliers:', out_rows)