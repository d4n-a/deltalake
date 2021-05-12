import pyspark
import pyspark.sql.functions as F

from delta.tables import *

'''
+-----+-------------------+-------------------+-----------+--------------------+
| name|          timestamp|          max_range| volume_sum|delta_total_percents|
+-----+-------------------+-------------------+-----------+--------------------+
|  XYL|2013-02-11 00:00:00| 0.5100002288818359|   875010.0|  -1.291030860351408|
|  URI|2013-02-15 00:00:00| 1.5499992370605469|  1970433.0|  1.5254886399796697|
|  PNW|2013-02-13 00:00:00| 0.3400001525878906|   912394.0|-0.07336925016780259|
| WYNN|2013-02-14 00:00:00| 1.7600021362304688|   887230.0| -0.3257279206383939|
|  BLL|2013-02-15 00:00:00|              0.375|  2723304.0| 0.17535733939158335|
|   IT|2013-02-11 00:00:00| 0.7800025939941406|   456923.0|  0.5026135737108927|
|   BA|2013-02-15 00:00:00| 0.5400009155273438|  3650898.0|  0.3056116800491947|
|  MCD|2013-02-14 00:00:00| 0.6999969482421875|  5360819.0| 0.31962822267947655|
|   ES|2013-02-14 00:00:00| 0.5400009155273438|  2865800.0|  0.6822675140053484|
|BRK.B|2013-02-12 00:00:00| 0.9799957275390625|  2385700.0| -0.6490133464495728|
|  ETR|2013-02-12 00:00:00| 0.3456001281738281|   884380.0| -0.2194691764212842|
|  ESS|2013-02-13 00:00:00|  1.079986572265625|   201597.0|  0.2870292921181766|
| FISV|2013-02-12 00:00:00| 0.2649993896484375|  1041038.0| 0.04964124425215365|
|   GE|2013-02-08 00:00:00|0.10999870300292969|2.4424506E7|                 0.0|
|  TXN|2013-02-14 00:00:00| 0.5400009155273438|  6394142.0| -0.8957874649897035|
|  LNC|2013-02-13 00:00:00|0.40000152587890625|  3307787.0| -0.9370839843033117|
|  STZ|2013-02-15 00:00:00|   1.19000244140625|1.0441047E7|-0.43981162813307395|
|  MPC|2013-02-15 00:00:00| 0.8349990844726562|  5677512.0| 0.30189591519201275|
|  FCX|2013-02-15 00:00:00| 0.7700004577636719|1.3721364E7|   1.545375764828577|
|  CAT|2013-02-14 00:00:00|                1.0|  3586223.0|-0.01041238443126...|
+-----+-------------------+-------------------+-----------+--------------------+
'''


spark = pyspark.sql.SparkSession.builder.master('spark://127.0.0.1:7077').appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
    .getOrCreate()

gold = DeltaTable.forPath(spark, "delta/gold/")
gold_df = gold.toDF()

print('gold_df:')
gold.show()

import plotly.graph_objects as go
import pandas as pd

names = ['WYNN', 'BLL', 'IT', 'BA']
for name in names:
    plot_data = gold_df.select(F.col('timestamp'), F.col('delta_total_percents')).where(F.col('name') == name)
    print(f'{name=}:')
    plot_data.show()

    import plotly.express as px

    fig = px.scatter(
        plot_data.toPandas(), x='timestamp', y='delta_total_percents', opacity=0.65,
        title=name,trendline='ols', trendline_color_override='darkblue'
    )
    fig.show()

