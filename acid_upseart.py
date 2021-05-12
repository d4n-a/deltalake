import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as stypes

from delta.tables import *

spark = pyspark.sql.SparkSession.builder.master('spark://127.0.0.1:7077').appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
    .getOrCreate()

processed = DeltaTable.forPath(spark, "delta/processed/")

'''
+----+-------------------+-------------------+-----------+--------------------+
|name|          timestamp|          max_range| volume_sum|delta_total_percents|
+----+-------------------+-------------------+-----------+--------------------+
|SWKS|2013-02-15 00:00:00| 0.6100006103515625|  2876718.0|  1.8036102171502972|
+----+-------------------+-------------------+-----------+--------------------+
'''

upseart_data = spark.read.format("csv").option("header", "true").load("upseart.csv")

processed.alias("processed").merge(
    upseart_data.alias("upseart"),
    "processed.name = upseart.name") \
  .whenMatchedUpdate(set = { "delta_total_percents" : "upseart.delta_total_percents" } ) \
  .whenNotMatchedInsert(values =
    {
        "name": "upseart.name",
        "timestamp": "upseart.timestamp",
        "max_range": "upseart.max_range",
        "volume_sum": "upseart.volume_sum",
        "delta_total_percents": "upseart.delta_total_percents",
    }
  ) \
  .execute()

processed.toDF().select('*').where(F.col("delta_total_percents") > 2).show()
