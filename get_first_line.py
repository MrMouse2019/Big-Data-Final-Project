import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('get_first_line').getOrCreate()

data = spark.read \
			.format('csv') \
			.load(sys.argv[1])

output = open('output.csv', 'a')
output.write(sys.argv[1] + ':\n')
output.write(data.head(2)[0][0] + '\n')
output.write(data.head(2)[1][0] + '\n')
output.write('\n')
output.close()
