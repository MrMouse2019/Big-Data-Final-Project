# get key candidates

import pandas as pd
import numpy as np

from itertools import combinations
import random

import sys
import os
import json


def getFilename(path):
	return path.split('/')[-1].split('.')[0]

def save(output, candidates, dataset_name):
	output['key_column_candidates'] = candidates
	with open('task1_key_cand/{}.json'.format(dataset_name), 'w') as outfile:
		json.dump(output, outfile, indent=4)


dataset_path = "test_data.tsv"
if len(sys.argv) >= 2:
	dataset_path = sys.argv[1]

dataset_name = getFilename(dataset_path)

df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n', nrows=2000)
columns = list(df.columns)

if len(sys.argv) >= 3:
	columns = sys.argv[2:]

#print ('Total Columns: ', len(columns))
#print (columns)

json_path = 'task1_key_cand/' + dataset_name + '.json'
if os.path.exists(json_path):
	print("Dataset: {} has been processed!".format(dataset_name))
	exit()

output = {}

output['dataset_name'] = dataset_name

df_dedup = df.drop_duplicates()
if len(df) != len(df_dedup):
	# if we have duplicate rows, there is no way to pick identify a single row even with all columns
	candidates = []
else:
	candidates = columns
	for cand_count in range(2, min(5, len(columns) + 1)):
		combs = list(combinations(columns, cand_count))
		for item in combs:
			cand = list(item)
			df1 = df[cand]
			df2 = df[cand].drop_duplicates()
			if len(df) == len(df_dedup):
				candidates = cand
				save(output, candidates, dataset_name)
				exit()
				
	for cand_count in range(5, min(11, len(columns) + 1)):
		cand = random.choices(columns, k=cand_count)
		df1 = df[cand]
		df2 = df[cand].drop_duplicates()
		if len(df) == len(df_dedup):
			candidates = cand
			save(output, candidates, dataset_name)
			exit()

save(output, candidates, dataset_name)
exit()

