import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, FloatType, BooleanType, TimestampType

spark = SparkSession.builder\
    .appName("IOT") \
    .master("local[2]") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()


iot_schema = StructType([
    StructField("row_id", IntegerType(), True),
    StructField("event_ts", DoubleType(), True),
    StructField("device", StringType(), True),
    StructField("co", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("light", BooleanType(), True),
    StructField("lpg", FloatType(), True),
    StructField("motion", BooleanType(), True),
    StructField("smoke", FloatType(), True),
    StructField("temp", FloatType(), True),
    StructField("generate_ts", TimestampType(), True)
])

lines = (spark.readStream
          .format("csv")
          .schema(iot_schema)
          .option("header", False)
          .option("maxFilesPerTrigger", 1)
          .load("file:///home/train/data-generator/output")
)


# For  required operations on event_ts we need to convert from timestamp to datetime.

lines2 = (lines.withColumn("ts_long", F.col("event_ts").cast("long")) \
          .withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("ts_long"))))
          .drop("ts_long"))


# Let's group the data in 10-minute windows and scroll every 5 minutes, aggregate it to print the necessary values on the screen, and assign it to the ten_min_avg variable.

ten_min_avg = lines2.groupBy(F.window(F.col("event_ts"), "10 minutes", "5 minutes"), F.col("device")).agg(F.count("*"), F.mean("co"), F.mean("humidity"))

checkpointDir = "file///home/train/checkpoint/iotCountCheckpoint"

# start Streaming
streamingQuery = (ten_min_avg.writeStream
                   .format("console")
                   .outputMode("complete")
                   .trigger(processingTime="1 second")
                   .option("numRows", 10)
                   .option("truncate", False)
                   .option("checkpointLocation", checkpointDir)
                   .start()
                   )

streamingQuery.awaitTermination()
