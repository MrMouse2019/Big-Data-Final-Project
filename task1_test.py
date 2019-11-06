import sys
import pyspark
import string
import json

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

from operator import add

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

	# get the header
	header = lines.first()

	# convert the header to an array
	headerArray = header.split('\t')
	num_columns = len(headerArray)

	# filter out the header
	header = lines.filter(lambda x: headerArray[0] in x)

	# no header
	lines0 = lines.subtract(header)

	# use spark_sql
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
		number_non_empty_cells = spark.sql("select count({}) as cnt from df".format(column_name)).collect()[0].cnt

		# number_empty_cells
		number_empty_cells = spark.sql("select count({}) as cnt from df where {} is NULL".format(column_name, column_name)).collect()[0].cnt

		# number_distinct_values
		number_distinct_values = spark.sql("select count(distinct({})) as cnt from df".format(column_name)).collect()[0].cnt

		# frequent_values
		frequent_values = spark.sql("select {} as value, count(*) as cnt from df group by {} order by cnt desc limit 5".format(column_name,column_name)).collect()
		frequent_values = [row.value for row in frequent_values]

		column_output = {}

		column_output['column_name'] = column_name
		column_output['number_non_empty_cells'] = number_non_empty_cells
		column_output['number_empty_cells'] = number_empty_cells
		column_output['number_distinct_values'] = number_distinct_values
		column_output['frequent_values'] = frequent_values
		column_output['data_type'] = []

		output['columns'].append(column_output)

	with open('{}.txt'.format(dataset_name), 'w') as outfile:
		json.dump(output, outfile, indent=4)

	sc.stop()