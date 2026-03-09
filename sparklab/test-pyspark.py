from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Zoomcamp_HW6").getOrCreate()

print(f"Spark version: {spark.version}")

df = spark.range(10)
df.show()

spark.stop()
