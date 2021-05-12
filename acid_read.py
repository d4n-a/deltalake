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

DTP_THRESHOLD = 2
processed.toDF().select('*').where(F.col("delta_total_percents") > DTP_THRESHOLD).show()
