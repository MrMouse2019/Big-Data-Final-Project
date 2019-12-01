import sys
import pyspark
import string
import json
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

from operator import add
from csv import reader
from itertools import islice
from datetime import datetime

def getFilename(path):
	return path.split('/')[-1].split('.')[0]

def is_int(val):
	try:
		int(val)
		return True
	except:
		return False

def is_real(val):
	try:
		float(val)
		return True
	except:
		return False

def get_number_meta_data(num_vals, dict):
	num_num = num_vals.count()
	if num_num == 0:
		return
	max_num = num_vals.max()
	min_num = num_vals.min()
	# avg_num = num_vals.sum() / num_num
	avg_num = num_vals.mean()
	#std_dev_num = (num_vals.map(lambda x: (x - avg_num) ** 2).sum() / num_num) ** 0.5
	std_dev_num = num_vals.stdev()
	# print("Max: %d, Min: %d, Mean: %.2f, Standard Deviation: %.2f" % (max_num, min_num, avg_num, std_dev_num))
	dict["count"] = num_num
	dict["max_value"] = max_num
	dict["min_value"] = min_num
	dict["mean"] = avg_num
	dict["stddev"] = std_dev_num
	return dict

def count_int(col):
	int_vals = col.filter(lambda x: is_int(x)).map(lambda x: int(x))
	dict = {"type": "INTEGER"}
	return get_number_meta_data(int_vals, dict)

def count_real(col):
	real_vals = col.filter(lambda x: not is_int(x)).filter(lambda x: is_real(x)).map(lambda x: float(x))
	dict = {"type": "REAL"}
	return get_number_meta_data(real_vals, dict)

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

def count_date(col):
	date_vals = col.filter(lambda x: is_date(x)).map(lambda x: to_date(x))
	num_date = date_vals.count()
	if num_date == 0:
		return
	max_date = date_vals.max()
	min_date = date_vals.min()
	output_format = "%m/%d/%Y, %H:%M:%S"
	# print("Number of Type Date: %d, Max: %s, Min: %s"
	# 	  % (num_date, max_date.strftime(output_format), min_date.strftime(output_format)))
	dict = {"type": "DATE/TIME"}
	dict["count"] = num_date
	dict["max_value"] = max_date.strftime(output_format)
	dict["min_value"] = min_date.strftime(output_format)
	return dict

def count_text(col):
	text_vals = col.filter(lambda x: not is_real(x))\
		.filter(lambda x: not is_date(x))\
		.map(lambda x: (x, len(x)))
	num_text = text_vals.count()
	if num_text == 0:
		return
	short_five = text_vals.takeOrdered(5, key = lambda x: x[1])
	long_five = text_vals.takeOrdered(5, key = lambda x: -x[1])
	avg_len_text = text_vals.map(lambda x: x[1]).sum() / num_text
	# print("Number of Type Text: %d" % num_text)
	# print(short_five)
	# print(long_five)
	# print("Average length: %.2f" % avg_len_text)
	dict = {"type": "TEXT"}
	dict["count"] = num_text
	dict["shortest_values"] = short_five
	dict["longest_values"] = long_five
	dict["average_length"] = avg_len_text
	return dict

if __name__ == "__main__":
	# default dataset path
	dataset_path = "/user/hm74/NYCOpenData/yrf7-4wry.tsv.gz"
	if len(sys.argv) >= 2:
		dataset_path = sys.argv[1]

	# determine if the json file of the dataset has existed
	dataset_name = getFilename(dataset_path)
	# path of the json file
	json_path = 'task1/' + dataset_name + '.json'
	if os.path.exists(json_path):
		print("Dataset: {} has been processed!".format(dataset_name))

	else:
		sc = SparkContext()

		spark = SparkSession \
			.builder \
			.appName("project") \
			.config("spark.some.config.option", "some-value") \
			.getOrCreate()

		sqlContext = SQLContext(spark)

		# read a tsv.gz file
		lines = sc.textFile(dataset_path)
		lines = lines.mapPartitions(lambda x: reader(x, delimiter='\t'))

		# column names array
		headerArray = lines.first()
		num_columns = len(headerArray)

		# remove the header
		lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

		# output
		output = {}
		# dataset_name: 2abb-gr8d
		output['dataset_name'] = dataset_name
		# columns
		output['columns'] = []
		# key_column_candidates
		output['key_column_candidates'] = []

		for i in range(num_columns):
			# column_name
			column_name = headerArray[i]

			# please comment this line to prevent encoding error, e.g.: '\u2013'
			# print("Processing Column: {}".format(column_name))

			col = lines.map(lambda x: x[i])

			# number_non_empty_cells
			number_non_empty_cells = col.filter(lambda x: x).count()

			# number_empty_cells
			number_empty_cells = col.filter(lambda x: not x).count()

			# number_distinct_values
			number_distinct_values = col.distinct().count()

			# frequent_values
			frequent_values = col.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).take(5)

			# data_types
			col = lines.map(lambda x: x[i]).filter(lambda x: x)  # filter out the None values in the column
			data_type = []
			ret = count_int(col)
			if ret != None:
				data_type.append(ret)
			ret = count_real(col)
			if ret != None:
				data_type.append(ret)
			ret = count_date(col)
			if ret != None:
				data_type.append(ret)
			ret = count_text(col)
			if ret != None:
				data_type.append(ret)

			column_output = {}
			column_output['column_name'] = column_name
			column_output['number_non_empty_cells'] = number_non_empty_cells
			column_output['number_empty_cells'] = number_empty_cells
			column_output['number_distinct_values'] = number_distinct_values
			column_output['frequent_values'] = frequent_values
			column_output['data_type'] = data_type
			column_output['semantic_types'] = []

			output['columns'].append(column_output)

		with open('task1/{}.json'.format(dataset_name), 'w') as outfile:
			json.dump(output, outfile, indent=4)

		sc.stop()