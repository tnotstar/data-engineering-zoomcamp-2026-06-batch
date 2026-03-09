from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Zoomcamp_HW6").getOrCreate()

# 0. Leer el CSV de zonas
df_zones = spark.read.option("header", "true").csv("../data/taxi_zone_lookup.csv")

# 1. Leer el archivo parquet
df_yellow = spark.read.parquet("../data/yellow_tripdata_2025-11.parquet")

# 2. Hacer join entre los viajes y las zonas usando el ID de la zona de origen (PULocationID)
df_joined = df_yellow.join(
    df_zones, df_yellow.PULocationID == df_zones.LocationID, how="inner"
)

# 3. Agrupar por nombre de la zona, contar, y ordenar de forma ascendente
df_joined.groupBy("Zone").count().orderBy("count", ascending=True).show(
    5, truncate=False
)

spark.stop()
