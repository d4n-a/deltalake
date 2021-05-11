import pyspark
import pyspark.sql.types as stypes

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

# r = df.groupby(df.amount).count()
print("write stream started")

#       date, open, high,  low, close, volume, name
# 2013-02-08,15.07,15.12,14.63,14.75,8407500,AAL
# 2013-02-11,14.89,15.01,14.26,14.46,8882000,AAL
# 2013-02-12,14.45,14.51, 14.1,14.27,8126000,AAL
# 2013-02-13,14.3,14.94,14.25,14.66,10259500,AAL
# import pyspark.sql.functions as func
#
#
# row = df.agg(
#    func.max(dfBasePrice.price_date).alias("maxDate"),
#    func.min(dfBasePrice.price_date).alias("minDate")
# ).collect()[0]
# startDate = row["minDate"]
# endDate = row["maxDate"]

'''
# Define our date range function
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

# Define combinePriceAndFund information by date and
def combinePriceAndFund(theDate):
  dfFund = dfBaseFund.where(dfBaseFund.price_date == theDate)
  dfPrice = dfBasePrice.where(
dfBasePrice.price_date == theDate
).drop('price_date')
  # Drop the updated column
  dfPriceWFund = dfPrice.join(dfFund, ['ticker']).drop('updated')

  # Save data to DBFS
   dfPriceWFund
.write
.format('delta')
.mode('append')
.save('/delta/stocksDailyPricesWFund')

# Loop through dates to complete fundamentals + price ETL process
for single_date in daterange(
startDate, (endDate + datetime.timedelta(days=1))
):
  print 'Starting ' + single_date.strftime('%Y-%m-%d')
  start = datetime.datetime.now()
  combinePriceAndFund(single_date)
  end = datetime.datetime.now()
  print (end - start)
'''

# query = df.writeStream.outputMode('append')\
#     .format('console').option('truncate', 'false').start()


#       date, open, high,  low, close, volume, name
# 2013-02-08,15.07,15.12,14.63,14.75,8407500,AAL
# 2013-02-11,14.89,15.01,14.26,14.46,8882000,AAL
# 2013-02-12,14.45,14.51, 14.1,14.27,8126000,AAL
# 2013-02-13,14.3,14.94,14.25,14.66,10259500,AAL

# query = df.groupby(df.date).count().writeStream.outputMode('update') \
#     .format('console').option('truncate', 'false').start()
df.printSchema()

# query = df.select('*').groupby(df.name).agg((F.col('open') - F.col('close') / F.col('open') * 100), F.max(F.col('high') - F.col('low')),  F.sum(df.volume))
query = df.withColumn('timestamp', F.unix_timestamp(F.col('date'), "yyyy-MM-dd").cast(stypes.TimestampType())) \
    .withWatermark("timestamp", "1 minutes") \
    .select('*').groupby(df.name, "timestamp").agg(
    F.max(F.col('high') - F.col('low')).alias('max_range'),
    F.sum(df.volume).alias('volume_sum'),
    F.sum((F.col('open') - F.col('close')) / F.col('open') * 100).alias('delta_total_percents')
)

# query = query.writeStream.outputMode('update') \
#     .format('console').option('truncate', 'false').start()
#
# query.awaitTermination()
#
# from delta.tables import *

# df2 = df_stream \
#               .withColumn('timestamp', unix_timestamp(col('EventDate'), "MM/dd/yyyy hh:mm:ss aa").cast(TimestampType())) \
#               .withWatermark("timestamp", "1 minutes") \
#               .groupBy(col("SendID"), "timestamp") \
#               .agg(max(col('timestamp')).alias("timestamp")) \
#               .orderBy('timestamp', ascending=False)
# or append?
query = query.writeStream.format("delta").outputMode("append") \
    .option("checkpointLocation", "checkpoints/etl-from-json") \
    .option("mergeSchema", "true") \
    .start("delta/events/")

print('query started')
query.awaitTermination()
print("success")


