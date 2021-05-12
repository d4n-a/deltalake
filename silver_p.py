import pyspark
import pyspark.sql.types as stypes

# first layer should read information from datalake

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from delta.tables import *
import pyspark.sql.functions as F

schema = stypes.StructType().add('date', stypes.DateType()) \
    .add('open', stypes.FloatType()).add('high', stypes.FloatType()) \
    .add('low', stypes.FloatType()).add('close', stypes.FloatType()) \
    .add('volume', stypes.FloatType()).add('name', stypes.StringType())

df = spark.readStream.format("delta").load("delta/bronze/")
print(df)

import pyspark
import pyspark.ml.feature as spark_features

column_pairs = [('open', F.col('close')),
                ('high', F.col('low')),
                ('low', F.col('high')),
                ('close', F.col('open')),
                ('volume', 0)
                ]

for column, target in column_pairs:
    df = df.withColumn(
        column,
        F.when(
            F.isnan(column) |
            F.col(column).isNull() |
            (F.col(column) == "NA") |
            (F.col(column) == "NULL"),
            target).otherwise(F.col(column)).cast(stypes.DoubleType()))


df.printSchema()

query = df.writeStream.format("delta").outputMode("append") \
    .option("checkpointLocation", "checkpoints/bronze-to-silver") \
    .option("mergeSchema", "true") \
    .start("delta/silver/")

print('query started')
query.awaitTermination()
print("success")