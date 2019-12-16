# test

import pandas as pd
import numpy as np

import sys

import spark

def freq_analy(val):
	freq_count = {'digit':0, 'letters': 0, 'whitespace':0, 'special char': 0}
	for c in val:
		if c.isdigit():
			freq_count['digit'] += 1
		elif c.isalpha():
			freq_count['letters'] += 1
		elif c == ' ':
			freq_count['whitespace'] += 1
		else:
			freq_count['special char'] += 1
	return freq_count

#dataset_path = 'p39r-nm7f.tsv.gz'
col_name = sys.argv[1]

df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n')

col_name = sys.argv[2]
#col_name = 'Case Name'

col = df[col_name]

col.value_counts()

stat = col.apply(freq_analy)

df

