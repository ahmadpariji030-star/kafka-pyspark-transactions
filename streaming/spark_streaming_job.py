from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    lit,
    expr,
    sum as spark_sum,
    window
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


KAFKA_BROKER = "localhost:9092"
SOURCE_TOPIC = "transactions"
CHECKPOINT_BASE = "./checkpoints"


spark = (
    SparkSession.builder
    .appName("TransactionStreamingValidation")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
])


# =========================
# 1. Read from Kafka
# =========================
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str")

parsed_df = (
    json_df
    .select(
        col("json_str"),
        from_json(col("json_str"), schema).alias("data")
    )
    .select(
        col("json_str"),
        col("data.user_id").alias("user_id"),
        col("data.amount").alias("amount"),
        col("data.timestamp").alias("timestamp_str"),
        col("data.source").alias("source")
    )
)

typed_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp_str")))


# =========================
# 2. Validations
# =========================
validated_df = (
    typed_df
    .withColumn(
        "error_reason",
        when(col("user_id").isNull() | (col("user_id") == ""), lit("mandatory_field_missing:user_id"))
        .when(col("amount").isNull(), lit("mandatory_field_missing:amount"))
        .when(col("timestamp_str").isNull() | (col("timestamp_str") == ""), lit("mandatory_field_missing:timestamp"))
        .when(col("event_time").isNull(), lit("invalid_type_or_timestamp_format"))
        .when((col("amount") < 1) | (col("amount") > 10000000), lit("amount_out_of_range"))
        .when(~col("source").isin("mobile", "web", "pos"), lit("invalid_source"))
        .when(col("event_time") < expr("current_timestamp() - INTERVAL 3 MINUTES"), lit("late_event_over_3m"))
        .otherwise(lit(None).cast("string"))
    )
    .withColumn(
        "is_valid_pre_dedup",
        when(col("error_reason").isNull(), lit(True)).otherwise(lit(False))
    )
)


# =========================
# 3. Valid + deduplicate
# =========================
candidate_valid_df = validated_df.filter(col("is_valid_pre_dedup") == True)

valid_df = (
    candidate_valid_df
    .withWatermark("event_time", "3 minutes")
    .dropDuplicates(["user_id", "event_time"])
    .withColumn("is_valid", lit(True))
    .withColumn("error_reason", lit(None).cast("string"))
    .select(
        "user_id",
        "amount",
        "timestamp_str",
        "source",
        "event_time",
        "is_valid",
        "error_reason"
    )
)

invalid_df = (
    validated_df
    .filter(col("is_valid_pre_dedup") == False)
    .withColumn("is_valid", lit(False))
    .select(
        "user_id",
        "amount",
        "timestamp_str",
        "source",
        "event_time",
        "is_valid",
        "error_reason"
    )
)


# =========================
# 4. Console output valid data
# =========================
valid_console_query = (
    valid_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/valid_console")
    .start()
)


# =========================
# 5. Console output invalid data
# =========================
invalid_console_query = (
    invalid_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/invalid_console")
    .start()
)


# =========================
# 6. Tumbling window monitoring
# =========================
window_monitor_df = (
    valid_df
    .withWatermark("event_time", "3 minutes")
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(
        spark_sum("amount").alias("total_amount_per_window")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_amount_per_window")
    )
)

window_query = (
    window_monitor_df.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/window_monitor")
    .start()
)


valid_console_query.awaitTermination()