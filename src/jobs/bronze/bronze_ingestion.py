from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, to_timestamp, date_format, coalesce


spark = SparkSession.builder.appName("bronze-payment-status-history").getOrCreate()



schema = StructType([
    StructField('event_id', StringType(), True),
    StructField('transaction_id', StringType(), True),
    StructField('intent_id', StringType(), True),
    StructField('charge_id', StringType(), True),
    StructField('refund_id', StringType(), True), 
    StructField('event_type', StringType(), True),
    StructField('chargeback_id', StringType(), True),
    StructField('status', StringType(), True),
    StructField('event_timestamp', StringType(), True),
    StructField('ingestion_timestamp', StringType(), True),
    StructField('source_system', StringType(), True),
    StructField('payload', StringType(), True),
])
                
                
                
kafka_df = (
    spark.readStream.format('kafka').option('kafka.bootstrap.servers','kafka:29092') \
    .option('subscribe', 'postgres-db.operational.payment_status_history') \
        .option('startingOffsets', 'latest') \
            .load()
)

parsed = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("r"))
    .select("r.*")
    # Parse ISO timestamps (with or without millis)
    .withColumn(
        "event_time",
        coalesce(
            to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
            to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ssX"),
        ),
    )
    .withColumn(
        "ingested_time",
        coalesce(
            to_timestamp(col("ingestion_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
            to_timestamp(col("ingestion_timestamp"), "yyyy-MM-dd'T'HH:mm:ssX"),
        ),
    )
    .withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
)


query = (
    
    parsed.writeStream \
        .format('delta') \
            .option('path', "s3a://lakehouse-bronze/payment_status_history") \
                .option("checkpointLocation", "s3a://lakehouse-bronze/_checkpoints/payment_status_history") \
                    .partitionBy('event_date') \
                        .outputMode('append') \
                            .start()

)

query.awaitTermination()