from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-master:15002").getOrCreate()
