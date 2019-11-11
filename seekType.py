import sys
import pyspark
import string
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

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
	avg_num = num_vals.sum() / num_num
	std_dev_num = (num_vals.map(lambda x: (x - avg_num) ** 2).sum() / num_num) ** 0.5
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
	get_number_meta_data(real_vals, dict)

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
	sc = SparkContext()

	spark = SparkSession \
		.builder \
		.appName("project") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	sqlContext = SQLContext(spark)

	inFile = "/user/hm74/NYCOpenData/rpeq-j89e.tsv.gz"
	if len(sys.argv) >= 2:
		inFile = sys.argv[1]

	# rdd = spark.read.format("csv").option("header", "true").load(inFile).rdd
	df = spark.read.csv(inFile, sep='\t', header='true')
	num_col = len(df.columns)
	print("Column number: %d"%num_col)
	rdd = df.rdd
	for col_seq in range(num_col):
		data_type = []
		print("Column: %d" % col_seq)
		col = rdd.map(lambda x: x[col_seq]).filter(lambda x: x != None)
		# a = col.take(10)
		# print(a)
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
		print(data_type)

	sc.stop()