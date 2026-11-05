"""Speed layer: Kafka -> Spark Structured Streaming -> (HDFS raw lake + Kafka metrics/alerts)."""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

BOOT, HDFS = "kafka:9092", "hdfs://namenode:9000"
spark = (SparkSession.builder.appName("SmartCityHealthEnv")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

def read(topic, schema):
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("subscribe", topic).option("startingOffsets", "latest").load())
    return (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
               .select("d.*").withColumn("ts", F.to_timestamp("ts")))

env = read("env", StructType([StructField("zone", StringType()), StructField("pm25", DoubleType()), StructField("ts", StringType())]))
traffic = read("traffic", StructType([StructField("segment", StringType()), StructField("zone", StringType()),
                                      StructField("speed", DoubleType()), StructField("vehicles", IntegerType()), StructField("ts", StringType())]))
health = read("health", StructType([StructField("patient_id", StringType()), StructField("zone", StringType()),
                                    StructField("heart_rate", IntegerType()), StructField("spo2", IntegerType()), StructField("ts", StringType())]))

def to_lake(df, name):   # batch layer feed: raw events on HDFS as Parquet
    return (df.writeStream.format("parquet").option("path", f"{HDFS}/lake/{name}")
            .option("checkpointLocation", f"{HDFS}/chk/{name}_raw").outputMode("append")
            .trigger(processingTime="20 seconds").start())

def to_kafka(df, name):  # live metrics + alerts for dashboard
    out = df.select(F.to_json(F.struct("*")).alias("value"))
    return (out.writeStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
            .option("topic", "city_metrics").option("checkpointLocation", f"{HDFS}/chk/{name}_metrics")
            .outputMode("update").trigger(processingTime="5 seconds").start())

W = lambda df: df.withWatermark("ts", "30 seconds")

traffic_m = (W(traffic).groupBy(F.window("ts", "30 seconds"), "segment")
             .agg(F.round(F.avg("speed"), 1).alias("value"))
             .select(F.lit("traffic").alias("domain"), F.col("segment").alias("key"),
                     F.col("window.end").alias("window_end"), "value")
             .withColumn("alert", F.col("value") < 20))          # congestion

env_m = (W(env).groupBy(F.window("ts", "30 seconds"), "zone")
         .agg(F.round(F.avg("pm25"), 1).alias("value"))
         .select(F.lit("environment").alias("domain"), F.col("zone").alias("key"),
                 F.col("window.end").alias("window_end"), "value")
         .withColumn("alert", F.col("value") > 60))              # pollution spike

health_m = (W(health).groupBy(F.window("ts", "30 seconds"), "zone")
            .agg(F.sum(F.when((F.col("heart_rate") > 120) | (F.col("spo2") < 92), 1).otherwise(0)).cast("double").alias("value"))
            .select(F.lit("health").alias("domain"), F.col("zone").alias("key"),
                    F.col("window.end").alias("window_end"), "value")
            .withColumn("alert", F.col("value") >= 3))           # abnormal vitals

for df, n in [(env, "env"), (traffic, "traffic"), (health, "health")]:
    to_lake(df, n)
for df, n in [(traffic_m, "traffic"), (env_m, "env"), (health_m, "health")]:
    to_kafka(df, n)

spark.streams.awaitAnyTermination()
