import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

from operator import add

# with gzip.open('2abb-gr8d.tsv.gz', 'rb') as f:
# 	for line in f:
# 		print(line)

if __name__ == "__main__":
	sc = SparkContext()

	spark = SparkSession \
		.builder \
		.appName("project") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	sqlContext = SQLContext(spark)

	# df.show()
	# df.createOrReplaceTempView("df")

	# read a tsv.gz file
	lines = sc.textFile("/user/hm74/NYCOpenData/2abb-gr8d.tsv.gz")

	# get the header
	header = lines.first()
	# convert the header to an array
	headerArray = header.split('\t')
	# filter out the header
	header = lines.filter(lambda x: headerArray[0] in x)
	# no header
	lines0 = lines.subtract(header)

	# use spark_sql
	df = spark.read.csv("/user/hm74/NYCOpenData/2abb-gr8d.tsv.gz", sep='\t', header='true')

	df.createOrReplaceTempView("df")


	for i in range(len(headerArray)):
		columnName = headerArray[i]
		spark.sql("select count(%s) as cnt from df" % columnName).show()

	# column_name
	columnName = "Week"

	# number_non_empty_cells
	number_non_empty_cells = spark.sql("select count({}) as cnt from df".format(columnName)).collect()[0].cnt

	# number_empty_cells
	number_empty_cells = spark.sql("select count({}) as cnt from df where {} is NULL".format(columnName, columnName)).collect()[0].cnt

	# number_distinct_values
	number_distinct_values = spark.sql("select count(distinct({})) as cnt from df".format(columnName)).collect()[0].cnt

	# frequent_values
	frequent_values = spark.sql("select {} as value, count(*) as cnt from df group by {} order by cnt desc limit 5".format(columnName, columnName)).collect()
	# frequent_values = [(row.value, row.cnt) for row in frequent_values]
	frequent_values = [row.value for row in frequent_values]


	sc.stop()