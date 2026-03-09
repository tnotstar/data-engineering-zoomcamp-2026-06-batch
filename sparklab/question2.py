from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Zoomcamp_HW6").getOrCreate()

# 1. Leer el archivo parquet
df_yellow = spark.read.parquet("../data/yellow_tripdata_2025-11.parquet")

# 2. Reparticionar a 4 y guardar como parquet en una nueva carpeta
df_yellow.repartition(4).write.parquet(
    "../data/output_yellow_2025_11", mode="overwrite"
)

spark.stop()
