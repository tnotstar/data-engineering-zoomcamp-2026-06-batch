from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("Zoomcamp_HW6").getOrCreate()

# 1. Leer el archivo parquet
df_yellow = spark.read.parquet("../data/yellow_tripdata_2025-11.parquet")

# Calculamos la diferencia en segundos y la convertimos a horas
df_duration = df_yellow.withColumn(
    "duration_hours",
    (
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime")
    )
    / 3600,
)

# Obtenemos el valor máximo
df_duration.select(F.max("duration_hours")).show()
