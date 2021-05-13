import pyspark
import pyspark.sql.functions as F

from delta.tables import *

spark = pyspark.sql.SparkSession.builder.master('spark://127.0.0.1:7077').appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
    .getOrCreate()

gold = DeltaTable.forPath(spark, "delta/gold/")
gold_history = gold.history()
print('gold history:')
gold_history.show()

# version = int(input("please, provide version to travel to: "))
version = 101
print(f'{version=}')

gold_timetravel = spark.read.format("delta").option("versionAsOf", version).load("delta/gold")

print(f"Chosen {version=}")
gold_timetravel.select('*').where(F.col('version') == version).show()

print(f"Data for that version:")
gold_timetravel.show()

print("DTP threshold 2 for that version:")
from read import show_df
show_df(gold_timetravel, 2)


