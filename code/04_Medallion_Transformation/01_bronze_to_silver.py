from pyspark.sql.functions import (
    col, to_date, date_format, trim, initcap,
    split, size, when, concat, lit, abs, to_timestamp, regexp_extract
)

catalog = "smart_claims_dev"
bronze_schema = "01_bronze"
silver_schema = "02_silver"

# --- CLEAN TELEMATICS ---
@dlt.table(
    name=f"{catalog}.{silver_schema}.telematics",
    comment="cleaned telematics events",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect("valid_coordinates", "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180")
def telematics():
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.telematics")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .drop("_rescued_data")
    )

# --- CLEAN POLICY ---
@dlt.table(
    name=f"{catalog}.{silver_schema}.policy",
    comment="Cleaned policies",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect("valid_policy_no", "policy_no IS NOT NULL")
def policy():
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.policy")
        .withColumn("premium", abs("premium"))
        .drop("_rescued_data")
    )

# --- CLEAN CLAIM ---
@dlt.table(
    name=f"{catalog}.{silver_schema}.claim",
    comment="Cleaned claims",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_claim_number": "claim_no IS NOT NULL",
    "valid_incident_hour": "incident_hour BETWEEN 0 AND 23"
})
def claim():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claim")
    return (
        df.withColumn("claim_date", to_date(col("claim_date")))
        .withColumn("incident_date", to_date(col("incident_date"), "yyyy-MM-dd"))
        .withColumn("license_issue_date", to_date(col("license_issue_date"), "dd-MM-yyyy"))
        .drop("_rescued_data")
    )

# --- CLEAN CUSTOMER ---
@dlt.table(
    name=f"{catalog}.{silver_schema}.customer",
    comment="Cleaned customers (split names, minimal checks, drop name)",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_customer_id": "customer_id IS NOT NULL"
})
def customer():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.customer")

    name_normalized = when(
        size(split(trim(col("name")), ",")) == 2,
        concat(
            initcap(trim(split(col("name"), ",").getItem(1))), lit(" "),
            initcap(trim(split(col("name"), ",").getItem(0)))
        )
    ).otherwise(initcap(trim(col("name"))))

    return (
        df
        .withColumn("date_of_birth", to_date(col("date_of_birth"), "dd-MM-yyyy"))
        .withColumn("firstname", split(name_normalized, " ").getItem(0))
        .withColumn("lastname", split(name_normalized, " ").getItem(1))
        .withColumn("address", concat(col("BOROUGH"), lit(", "), col("ZIP_CODE")))
        .drop("name", "_rescued_data")
    )

# --- CLEAN TRAINING IMAGES ---
@dlt.table(
    name=f"{catalog}.{silver_schema}.training_images",
    comment="Enriched accident training images",
    table_properties={
        "quality": "silver"
    }
)
def training_images():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.training_images")
    return df.withColumn(
        "label",
        regexp_extract("path", r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2)
    )

# --- CLEAN CLAIM IMAGES ---
@dlt.table(
    name=f"{catalog}.{silver_schema}.claim_images",
    comment="Enriched claim images",
    table_properties={
        "quality": "silver"
    }
)
def training_images():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claim_images")
    return df.withColumn("image_name", regexp_extract(col("path"), r".*/(.*?.jpg)", 1))
