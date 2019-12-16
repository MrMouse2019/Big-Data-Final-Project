import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat, col, lit

if __name__ == "__main__":
    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("project") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    inFile = "Legally_Operating_Businesses.csv"

    df = spark.read.csv(inFile, header='true')
    df = df.filter("BusinessName is not NULL")\
        .select("BusinessName", "BusinessName2")\
        .orderBy("BusinessName") \
        .distinct()

    rdd = df.rdd
    rdd = rdd.map(lambda x: (x[0] + ' ' + x[1]) if x[1] else x[0]).saveAsTextFile("BusinessNames.out")
