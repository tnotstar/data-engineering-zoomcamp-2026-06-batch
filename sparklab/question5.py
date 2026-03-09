import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Zoomcamp_HW6").getOrCreate()

os.system("ss -tunl")
_ = input("Press <ENTER> to session exit...")

spark.stop()
