from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("Zoomcamp_HW6").getOrCreate()

# 1. Leer el archivo parquet
df_yellow = spark.read.parquet("../data/yellow_tripdata_2025-11.parquet")

# 2.Filtramos donde la fecha de inicio coincida con '2025-11-15'
df_nov_15 = df_yellow.filter(F.to_date(df_yellow.tpep_pickup_datetime) == "2025-11-15")

# Contamos los registros
trips_count = df_nov_15.count()
print(f"Viajes el 15 de Noviembre: {trips_count}")
