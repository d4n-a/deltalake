import pyspark
import pyspark.sql.types as stypes

# first layer should read information from datalake

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from delta.tables import *

# stream = spark.readStream.format("delta").schema(schema).load("/delta/events")

schema = stypes.StructType().add('date', stypes.DateType()) \
    .add('open', stypes.FloatType()).add('high', stypes.FloatType()) \
    .add('low', stypes.FloatType()).add('close', stypes.FloatType()) \
    .add('volume', stypes.FloatType()).add('name', stypes.StringType())

df = spark.readStream.format("delta").load("delta/events/")
print(df)

# stream.show()
# print(stream.count())
#
# import time
# time.sleep(10)
# print(stream.count())

df.printSchema()

# query = .writeStream.format("delta").outputMode("complete") \
#     .option("checkpointLocation", "checkpoints/etl-from-json") \
#     .option("mergeSchema", "true") \
#     .start("delta/events/")

query = df.writeStream.outputMode('update') \
    .format('console').option('truncate', 'false').start()

query.awaitTermination()


# query = stream.writeStream.format("delta").outputMode("append") \
#     .option("checkpointLocation", "checkpoints/etl-to-delta") \
#     .option("mergeSchema", "true") \
#     .start("delta/processed_events/")
#
# print('query started')
# query.awaitTermination()
# print("success")

