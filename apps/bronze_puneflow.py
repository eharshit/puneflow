# pyrefly: ignore-errors
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark Session Setup

spark = (
    SparkSession.builder
    .appName("Puneflow_Lakehouse")
    # Master Node
    .master("spark://spark-master:7077")
    # Delta lake 
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Kafka Raw Stream Ingestion

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "pune-traffic-topic")
    .option("startingOffsets", "latest")
    .load()
)

# Convert Binary to String 

json_stream = raw_stream.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "timestamp as kafka_timestamp"
)

# Flexible Schema
traffic_schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("road_name", StringType(), True),
    StructField("pune_area", StringType(), True),
    StructField("speed", StringType(), True),
    StructField("congestion_level", IntegerType(), True),
    StructField("weather", StringType(), True),
    StructField("event_time", StringType(), True)
])

parsed = json_stream.withColumn(
    "data",
    from_json(col("raw_json"), traffic_schema)
)

flattened = parsed.select(
    "raw_json",
    "kafka_timestamp",
    "data.*"
)

bronze_query = (
    flattened.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_bronze")
    .option("path", "/opt/spark/warehouse/traffic_bronze")
    .start()
)

spark.streams.awaitAnyTermination()