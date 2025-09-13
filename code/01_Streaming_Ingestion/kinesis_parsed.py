import dlt
from pyspark.sql.functions import from_json, col, struct
from pyspark.sql.types import MapType, StringType

kinesis_config = {
    "streamName": "<telematics-stream-name>",
    "region": "<region>",
    "serviceCredential": "<kinesis-service-credential-name>",
    "initialPosition": "earlierst"
}

payload_schema = MapType(StringType(), StringType())

@dlt.table(
    name="telematics",
    comment="Parsed Kinesis data as map with metadata struct",
    table_properties={"quality": "bronze"}
)
def bronze_table():
    raw_stream = (
        spark.readStream
        .format("kinesis")
        .options(**kinesis_config)
        .load()
    )

    parsed_stream = raw_stream.selectExpr(
        "CAST(data AS STRING) AS raw_json",
        "partitionKey",
        "stream",
        "shardId",
        "sequenceNumber",
        "approximateArrivalTimestamp"
    ).withColumn(
        "decoded_data",
        from_json(col("raw_json"), payload_schema)
    ).withColumn(
        "stream_metadata",
        struct(
            col("partitionKey"),
            col("stream"),
            col("shardId"),
            col("sequenceNumber"),
            col("approximateArrivalTimestamp")
        )
    )

    return parsed_stream.select(
            col("decoded_data.chassis_no"),
            col("decoded_data.latitude"),
            col("decoded_data.longitude"),
            col("decoded_data.event_timestamp"),
            col("decoded_data.speed"),
            col("stream_metadata")
        )