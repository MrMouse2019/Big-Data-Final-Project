import pandas as pd
import numpy as np

import sys

dataset_path = "test_data.tsv"
if len(sys.argv) >= 2:
	dataset_path = sys.argv[1]


df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n')

total_rows = len(df)
print ('Total Rows: ', total_rows)
print ('Total Columns: ', len(df.columns))


# 11962105L