from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, to_timestamp, date_format, coalesce

APP_NAME = "bronze-payment-status-history"

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "postgres-db.operational.payment_status_history"

BRONZE_PATH = "s3a://lakehouse-bronze/payment_status_history"
CHECKPOINT_PATH = "s3a://lakehouse-bronze/_checkpoints/payment_status_history"



def create_spark_session(app_name: str = APP_NAME) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def create_schema() -> StructType:
    
    return StructType([
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
                
                
def read_kafka_stream(spark: SparkSession, bootstrap:str, topic:str) -> DataFrame:
    
    
    return spark.readStream.format('kafka').option('kafka.bootstrap.servers',bootstrap) \
        .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
                .option('failOnDataLoss', 'false') \
                    .load()
    
def format_bronze_payload(raw_df: DataFrame, schema: StructType) -> DataFrame:
    
    return (
        raw_df
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

def write_delta_stream(df, path:str, checkpoint:str):
    return (
        
        df.writeStream \
            .format('delta') \
                .option('path', path) \
                    .option("checkpointLocation", checkpoint) \
                        .partitionBy('event_date') \
                            .outputMode('append') \
                                .start()

    )
def main():
    spark = create_spark_session()
    Schema = create_schema()
    
    kafka_df = read_kafka_stream(spark, KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    parsed_df = format_bronze_payload(kafka_df, Schema)
    
    query = write_delta_stream(parsed_df, BRONZE_PATH, CHECKPOINT_PATH)


    query.awaitTermination()
    
if __name__ == "__main__":
    main()