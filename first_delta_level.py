import pyspark
import pyspark.sql.types as stypes

# first layer should read information from datalake

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from delta.tables import *

schema = stypes.StructType().add('date', stypes.DateType()) \
    .add('open', stypes.FloatType()).add('high', stypes.FloatType()) \
    .add('low', stypes.FloatType()).add('close', stypes.FloatType()) \
    .add('volume', stypes.FloatType()).add('name', stypes.StringType())

df = spark.readStream.format("delta").load("delta/events/")
print(df)

df.printSchema()

query = df.writeStream.outputMode('update') \
    .format('console').option('truncate', 'false').start()

query.awaitTermination()