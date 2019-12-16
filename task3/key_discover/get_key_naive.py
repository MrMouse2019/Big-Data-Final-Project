# get key candidates

import pandas as pd
import numpy as np

from itertools import combinations
import random

import sys
import os
import json

# load task1.json
with open('task1.json', 'r') as json_file:
	data = json.load(json_file)

for dataset in data['datasets']:
	for column in dataset['columns']:
		if column['number_distinct_values'] == column['number_non_empty_cells'] and column['number_empty_cells'] == 0:
			output = {}
			dataset_name = dataset['dataset_name']
			output['dataset_name'] = dataset_name
			output['key_column_candidates'] = [column['column_name']]
			json_path = 'task1_key_cand/' + dataset_name + '.json'
			with open('task1_key_cand/{}.json'.format(dataset_name), 'w') as outfile:
				json.dump(output, outfile, indent=4)
