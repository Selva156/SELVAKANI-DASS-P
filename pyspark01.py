import os
os.environ['SPARK_HOME'] = "C:\Users\HP\Spark-model"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("sample01") \
    .getOrCreate()

data = [("sanjay", 36), ("amar", 23), ("selva", 43)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()