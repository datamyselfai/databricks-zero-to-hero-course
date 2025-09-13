@dlt.table(
    name="smart_claims_dev.01_bronze.training_images",
    comment="Raw accident training image ingested from S3", 
    table_properties={"quality": "bronze"}
)
def raw_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "BINARYFILE")
        .load(f"/Volumes/smart_claims_dev/00_landing/training_imgs"))