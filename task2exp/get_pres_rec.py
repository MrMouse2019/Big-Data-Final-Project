import json
import pandas as pd

semantic_types = []

with open('label_list', 'r') as input_file:
	for line in input_file:
		semantic_types.append(line[:-1])

with open('task2.json', 'r') as pred_file:
	pred = json.load(pred_file)

with open('task2-manual-labels.json', 'r') as label_file:
	label = json.load(label_file)

pred_dict = {}  # {col_name:[pred_type1, pred_type2, ..]}

for col in pred['predicted_types']:
	col_name = col['column_name']
	for pred_label in col['semantic_types']:
		if pred_dict.get(col_name):
			pred_dict[col_name].append(pred_label['semantic_type'])
		else:
			pred_dict[col_name] = []
			pred_dict[col_name].append(pred_label['semantic_type'])

label_dict = {}  # {col_name:[label_type1, label_type2, ..]}

for col in label['actual_types']:
	col_name = col['column_name']
	for manual_label in col['manual_labels']:
		if label_dict.get(col_name):
			label_dict[col_name].append(manual_label['semantic_type'])
		else:
			label_dict[col_name] = []
			label_dict[col_name].append(manual_label['semantic_type'])

columns = ['Semantic Type', 'Precision', 'Recall']
result = pd.DataFrame(columns=columns)

for type in semantic_types:
	correct_pred = 0
	tol_col_type = 0
	tol_col_pred_type = 0

	for col in pred_dict:
		for pred_type in pred_dict[col]:
			if pred_type == type:
				tol_col_pred_type += 1
				for label_type in label_dict[col]:
					if label_type == type:
						correct_pred += 1

	for col in label_dict:
		for label_type in label_dict[col]:
			if label_type == type:
				tol_col_type += 1

	if tol_col_type and tol_col_pred_type:
		precision = float(correct_pred)/tol_col_pred_type
		recall = float(correct_pred)/tol_col_type

		result.loc[len(result)] = [type, precision, recall]

print (result)

