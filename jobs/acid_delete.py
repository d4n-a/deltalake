import pyspark
import pyspark.sql.functions as F

from delta.tables import *

spark = pyspark.sql.SparkSession.builder.master('spark://127.0.0.1:7077').appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
    .getOrCreate()

gold = DeltaTable.forPath(spark, "../delta/gold/")

gold.delete(F.abs(F.col("delta_total_percents")) > 2)   # predicate using Spark SQL functions

gold.toDF().select('*').where(F.col("delta_total_percents") > 1.7).show()

