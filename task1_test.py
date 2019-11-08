import sys
import pyspark
import string
import json

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from operator import add
from csv import reader
from itertools import islice

def getFilename(path):
	return path.split('/')[-1].split('.')[0]

if __name__ == "__main__":
	sc = SparkContext()

	spark = SparkSession \
		.builder \
		.appName("project") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	sqlContext = SQLContext(spark)

	# read a tsv.gz file
	# lines = sc.textFile("/user/hm74/NYCOpenData/2abb-gr8d.tsv.gz")
	dataset_path = sys.argv[1]
	lines = sc.textFile(dataset_path)
	lines = lines.mapPartitions(lambda x: reader(x, delimiter='\t'))

	# column names array
	headerArray = lines.first()
	num_columns = len(headerArray)

	# remove the header
	lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

	# create dataframe and then use spark-sql
	df = spark.read.csv(dataset_path, sep='\t', header='true')
	df.createOrReplaceTempView("df")

	# output
	output = {}
	# dataset_name: 2abb-gr8d
	dataset_name = getFilename(dataset_path)
	output['dataset_name'] = dataset_name
	# columns
	output['columns'] = []
	# key_column_candidates
	output['key_column_candidates'] = []

	for i in range(num_columns):
		# column_name
		column_name = headerArray[i]

		# number_non_empty_cells
		# number_non_empty_cells = spark.sql("select count({}) as cnt from df".format(column_name)).collect()[0].cnt
		number_non_empty_cells = lines.filter(lambda x: x).count()

		# number_empty_cells
		# number_empty_cells = spark.sql("select count({}) as cnt from df where {} is NULL".format(column_name, column_name)).collect()[0].cnt
		number_empty_cells = lines.filter(lambda x: not x).count()

		# number_distinct_values
		number_distinct_values = spark.sql("select count(distinct({})) as cnt from df".format(column_name)).collect()[0].cnt

		# frequent_values
		frequent_values = spark.sql("select {} as value, count(*) as cnt from df group by {} order by cnt desc limit 5".format(column_name,column_name)).collect()
		frequent_values = [row.value for row in frequent_values]

		# data_types
		columni = lines.map(lambda x: x[i])

		# Integer
		count_int = 0
		int_max_value = -sys.maxsize
		int_min_value = sys.maxsize
		int_mean = 0.0
		int_stddev = 0.0

		# Float
		count_float = 0
		float_max_value = -float("inf")
		float_min_value = float("inf")
		float_mean = 0.0
		float_stddev = 0.0

		# Datetime
		count_datetime = 0
		datetime_max_value = ""
		datetime_min_value = ""

		# String
		count_string = 0
		shortest_values = []
		longest_values = []
		average_length = 0.0

		# Integer
		# Float
		# Datetime
		# String
		if isinstance(df.schema[column_name].dataType, StringType):

			# filter out non-text values in the column
			filtered_columni = columni.filter(lambda x: type(x) is str)

			# count: the number of values of type TEXT in the column
			count_string = filtered_columni.count()

			# shortest_values: a list with the top-5 shortest values
			shortest_values = filtered_columni.map(lambda x: (x, len(x))).sortBy(lambda x: x[1]).map(lambda x: x[0]).take(5)

			# longest_values
			longest_values = filtered_columni.map(lambda x: (x, len(x))).sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).take(5)

			# average_length
			total_length = filtered_columni.map(lambda x: len(x)).reduce(add)
			average_length = total_length / count_string

		column_output = {}

		column_output['column_name'] = column_name
		column_output['number_non_empty_cells'] = number_non_empty_cells
		column_output['number_empty_cells'] = number_empty_cells
		column_output['number_distinct_values'] = number_distinct_values
		column_output['frequent_values'] = frequent_values

		column_output['data_type'] = []
		temp = {}
		# INTEGER (LONG)
		if count_int > 0:
			temp = {}
			temp['type'] = 'INTEGER (LONG)'
			temp['count'] = count_int
			temp['max_value'] = int_max_value
			temp['min_value'] = int_min_value
			temp['mean'] = int_mean
			temp['stddev'] = int_stddev
			column_output['data_type'].append(temp)
		# Real
		if count_float > 0:
			temp = {}
			temp['type'] = 'Real'
			temp['count'] = count_float
			temp['max_value'] = float_max_value
			temp['min_value'] = float_min_value
			temp['mean'] = float_mean
			temp['stddev'] = float_stddev
			column_output['data_type'].append(temp)
		# DATE/TIME
		if count_datetime > 0:
			temp = {}
			temp['type'] = 'DATE/TIME'
			temp['count'] = count_datetime
			temp['max_value'] = datetime_max_value
			temp['min_value'] = datetime_min_value
			column_output['data_type'].append(temp)
		# TEXT
		if count_string > 0:
			temp = {}
			temp['type'] = 'TEXT'
			temp['count'] = count_string
			temp['shortest_values'] = shortest_values
			temp['longest_values'] = longest_values
			temp['average_length'] = average_length
			column_output['data_type'].append(temp)

		column_output['semantic_types'] = []

		output['columns'].append(column_output)

	with open('{}.txt'.format(dataset_name), 'w') as outfile:
		json.dump(output, outfile, indent=4)

	sc.stop()