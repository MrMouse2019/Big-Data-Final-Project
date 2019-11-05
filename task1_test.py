import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

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

	df = spark.read.csv("/user/hm74/NYCOpenData/2abb-gr8d.tsv.gz", sep='\t', header='true')
	# df.show()
	df.createOrReplaceTempView("df")

	spark.sql("select type_name(Week), count(*) as cnt from df group by type_name(Week)").show()


	sc.stop()