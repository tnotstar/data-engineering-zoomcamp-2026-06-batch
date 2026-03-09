import pyspark
from pyspark.sql import SparkSession

# Creamos la sesión local de Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Zoomcamp_HW6") \
    .getOrCreate()

# Imprimimos la versión
print(spark.version)
