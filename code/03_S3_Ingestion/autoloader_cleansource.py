# Define paths
landing_catalog = "smart_claims_dev"
landing_schema = "00_landing"

base_path = f"/Volumes/{landing_catalog}/{landing_schema}/claims"
source_path = f"{base_path}/images"
archive_path = f"{base_path}/archive"
metadata_path = f"{base_path}/autoloader_metadata"

# Archive configs
archive_configs = {
    "cloudFiles.cleanSource": "MOVE", # OR DELETE
    "cloudFiles.cleanSource.retentionDuration": "1 minute", # MOVE AFTER 1 MIN - FOR DELETE MIN. 7 DAYS
    "cloudFiles.cleanSource.moveDestination": archive_path
}

claim_images_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", f"'{metadata_path}/_schema")
    .options(**archive_configs)
    .load(source_path)
)

(
    claim_images_df.writeStream
    .option("checkpointLocation", f"{metadata_path}/_checkpoint")
    .trigger(availableNow=True)
    .toTable("smart_claims_dev.01_bronze.claim_images")
)