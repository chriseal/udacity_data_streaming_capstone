import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# DONE Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', StringType(), False),
    StructField('original_crime_type_name', StringType()),
    StructField('report_date', StringType()),
    StructField('call_date', StringType()),
    StructField('offense_date', StringType()),
    StructField('call_time', StringType()),
    StructField('call_date_time', StringType()),
    StructField('disposition', StringType()),
    StructField('address', StringType()),
    StructField('city', StringType()),
    StructField('state', StringType()),
    StructField('agency_id', StringType()),
    StructField('address_type', StringType()),
    StructField('common_location', StringType())
])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police.call.service") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

        # .option("maxRatePerPartition", 100) \     
        # NOTE: spark.streaming.kafka.maxRatePerPartition for Direct Kafka approach. In Spark 1.5, we have introduced a feature called backpressure that eliminate the need to set this rate limit, as Spark Streaming automatically figures out the rate limits and dynamically adjusts them if the processing conditions change. This backpressure can be enabled by setting the configuration parameter spark.streaming.backpressure.enabled to true. https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html
    
    # Show schema for the incoming resources for checks
    df.printSchema()

    # DONE extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS string)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    service_table.printSchema()

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table. \
        select(
            psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
            psf.col("original_crime_type_name"), 
            psf.col("disposition")
        )
    distinct_table.printSchema()
    
    # count the number of original crime type
    agg_df = distinct_table \
        .select(
            psf.col('original_crime_type_name'), 
            psf.col('call_date_time'),
            psf.col('disposition')
        ) \
        .groupby(
            psf.window(psf.col('call_date_time'), "15 minutes"),
            psf.col('original_crime_type_name')
        ) \
        .count()
    
    agg_df.printSchema()
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df.writeStream.format("console").outputMode("Complete").trigger(processingTime="15 seconds").start()
    
    # TODO attach a ProgressReporter
    query.awaitTermination()
    
    if False:
        # TODO get the right radio code json path
        radio_code_json_filepath = "./radio_code.json"
        radio_code_df = spark.read.json(radio_code_json_filepath)

        # clean up your data so that the column names match on radio_code_df and agg_df
        # we will want to join on the disposition code

        # TODO rename disposition_code column to disposition
        radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

        # TODO join on disposition column
        join_query = agg_df.join(radio_code_df, "disposition")

        # join_query.writeStream.format("console").outputMode("complete").trigger(processingTime="15 seconds").start()
        # join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .config("spark.ui.port", 3000) \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
