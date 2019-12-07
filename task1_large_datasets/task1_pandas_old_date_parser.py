import pandas as pd
import numpy as np
import datetime
import sys
import os
import json
import time
from dateutil.parser import *


def getFilename(path):
    return path.split('/')[-1].split('.')[0]

def is_int(val):
    try:
        int(val)
        return True
    except:
        return False

def is_real(val):
    if is_int(val):
        return False
    try:
        float(val)
        return True
    except:
        return False

def is_date(val):
    date_formats = ["%m/%d/%Y %I:%M:%S %p", "%Y %b %d %I:%M:%S %p", "%m/%d/%Y"]
    for date_format in date_formats:
        try:
            datetime_object = datetime.strptime(val, date_format)
            return True
        except:
            pass
    return False

def to_date(val):
    date_formats = ["%m/%d/%Y %I:%M:%S %p", "%Y %b %d %I:%M:%S %p", "%m/%d/%Y"]
    for date_format in date_formats:
        try:
            return datetime.strptime(val, date_format)
        except:
            pass

def is_text(val):
    if is_int(val):
        return False
    if is_real(val):
        return False
    if is_date(val):
        return False
    return True

f = open('large_size_datasets.txt', 'r')
large_datasets = []
for line in f:
    large_datasets.append(line.split('/')[-1].replace('\n', ''))
f.close()

dataset_dir = 'large_datasets/'
for dataset in large_datasets:
    start_time = time.time()
    start_time1 = time.ctime()
    dataset_name = dataset
    dataset_path = dataset_dir + dataset_name
    print("Testing dataset:{} at time {}".format(dataset_name, start_time1))

    json_path = 'task1/' + dataset_name + '.json'
    if os.path.exists(json_path):
        print("Dataset: {} has been processed!".format(dataset_name))
        continue

    df = pd.read_csv(dataset_path, sep='\t', lineterminator='\n')
    total_rows = len(df)
    len_col = len(df.columns)
    print ('Total Rows: ', total_rows)
    print ('Total Columns: ', len_col)

    output = {}
    output['dataset_name'] = dataset_name
    # columns
    output['columns'] = []
    # key_column_candidates
    output['key_column_candidates'] = []


    col_id = 0
    for col in df.columns:
        column_output = {}

        column_output['column_name'] = col
        col_id += 1
        # print (col, time.ctime())
        print("Processing column {}/{}...{}".format(col_id, len_col, time.ctime()))

        frequent_values = list(df[col].value_counts().nlargest(n=5).index)

        number_empty_cells = len((np.where(pd.isnull(df[col])))[0])
        number_non_empty_cells = total_rows - number_empty_cells
        number_distinct_values = len(pd.unique(df[col]))

        data_type = []

        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.isdigit.html
        int_rows_idx = np.where(df[col].apply(is_int))[0]
        real_rows_idx = np.where(df[col].apply(is_real))[0]
        date_rows_idx = np.where(df[col].apply(is_date))[0]
        text_rows_idx = np.where(df[col].apply(is_text))[0]

        int_rows_cnt = len(int_rows_idx)
        real_rows_cnt = len(real_rows_idx)
        date_rows_cnt = len(date_rows_idx)
        text_rows_cnt = len(text_rows_idx)

        # INTEGER
        if int_rows_cnt:
            int_rows = df[col].iloc[int_rows_idx].astype('int64')
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
            real_rows = df[col].iloc[real_rows_idx].astype('float')
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
            date_rows = df[col].iloc[date_rows_idx].astype('datetime64[ns]')
            dict_date["type"] = "DATE/TIME"
            dict_date["count"] = date_rows_cnt
            dict_date["max_value"] = date_rows.max()
            dict_date["min_value"] = date_rows.min()
            data_type.append(dict_date)

        # TEXT
        if text_rows_cnt:
            dict_text = {}
            text_rows_dup = df[col].iloc[text_rows_idx]
            text_rows = pd.Series(pd.unique(text_rows_dup))

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
            dict_text["average_length"] = float(text_rows_dup.str.len().mean())
            data_type.append(dict_text)

    with open('task1_large/{}.json'.format(dataset_name), 'w') as outfile:
        json.dump(output, outfile, indent=4)

    end_time = time.time()
    end_time1 = time.ctime()
    print("Ending time: {}".format(end_time1))
    print("Time cost: {}".format(str(datetime.timedelta(seconds=(end_time - start_time)))))

