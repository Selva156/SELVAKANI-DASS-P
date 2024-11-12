import os
os.environ['SPARK_HOME'] = "C:\Users\HP\Spark-model"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark import SparkContext

sc = SparkContext(appName="MySparkApplication")

sc

sc.stop()

spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .getOrCreate()

sc = spark.sparkContext

sc

sc.stop()