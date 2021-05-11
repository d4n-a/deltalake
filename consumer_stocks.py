import pyspark
import pyspark.sql.types as stypes

# TODO: сделать того кто будет читать из базы обрабатывать и писать в базу

print("start creating spark connection")

spark = pyspark.sql.SparkSession.builder.master('spark://127.0.0.1:7077').appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
    .getOrCreate()

import pyspark.sql.functions as F

schema = stypes.StructType().add('date', stypes.DateType()) \
    .add('open', stypes.FloatType()).add('high', stypes.FloatType()) \
    .add('low', stypes.FloatType()).add('close', stypes.FloatType()) \
    .add('volume', stypes.FloatType()).add('name', stypes.StringType())

print("schema defined ")

df = spark.readStream.option("host","localhost").option("port","9999") \
    .option('includeTimestamp', 'true').schema(schema).csv('datasource')
    # .option('includeTimestamp', 'true').csv('datasource')

df.printSchema()

print("write stream started")

df.printSchema()

query = df.withColumn('timestamp', F.unix_timestamp(F.col('date'), "yyyy-MM-dd").cast(stypes.TimestampType())) \
    .withWatermark("timestamp", "1 minutes") \
    .select('*').groupby(df.name, "timestamp").agg(
    F.max(F.col('high') - F.col('low')).alias('max_range'),
    F.sum(df.volume).alias('volume_sum'),
    F.sum((F.col('open') - F.col('close')) / F.col('open') * 100).alias('delta_total_percents')
)

query = query.writeStream.format("delta").outputMode("append") \
    .option("checkpointLocation", "checkpoints/etl-from-json") \
    .option("mergeSchema", "true") \
    .start("delta/events/")

print('query started')
query.awaitTermination()
print("success")


