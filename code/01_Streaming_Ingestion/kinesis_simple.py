import dlt
from pyspark.sql.functions import from_json, col, struct 
from pyspark.sql.types import MapType, StringType

kinesis_config = {
    "streamName": "<telematics-stream-name>",
    "region": "<region>",
    "serviceCredential": "<kinesis-service-credential-name>",
    "initialPosition": "earlierst"
}

def bronze_table():
    raw_stream = (
        spark.readStream
        •format("kinesis")
        •options(**kinesis_config)
        •load ()
        )
    return raw_stream;