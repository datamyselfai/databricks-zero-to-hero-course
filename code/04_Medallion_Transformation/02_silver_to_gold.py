import geopy
import pandas as pd
from pyspark.sql.functions import col, lit, concat, pandas_udf, avg
from typing import Iterator
import random

catalog = 'smart_claims_dev'
silver_schema = '02_silver'
gold_schema = '03_gold'

def geocode(geolocator, address):
    try:
        #Skip the API call for faster demo (remove this line for ream)
        return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
        location = geolocator.geocode(address)
        if location:
            return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
        print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})
      
@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  #ctx = ssl.create_default_context(cafile=certifi.where())
  #geopy.geocoders.options.default_ssl_context = ctx
  geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
  for address in batch_iter:
    yield address.apply(lambda x: geocode(geolocator, x))

@dlt.table(
    name=f"{catalog}.{gold_schema}.aggregated_telematics",
    comment="Average telematics",
    table_properties={
        "quality": "gold"
    }
)
def telematics():
    return (
        dlt.read(f"{catalog}.{silver_schema}.telematics")
        .groupBy("chassis_no")
        .agg(
            avg("speed").alias("telematics_speed"),
            avg("latitude").alias("telematics_latitude"),
            avg("longitude").alias("telematics_longitude"),
        )
    )

# --- CLAIM-POLICY ---
@dlt.table(
    name=f"{catalog}.{gold_schema}.customer_claim_policy",
    comment = "Curated claim joined with policy records",
    table_properties={
        "quality": "gold"
    }
)
def customer_claim_policy():
    # Read the cleaned policy records
    policy = dlt.readStream(f"{catalog}.{silver_schema}.policy")
    # Read the cleaned claim records
    claim = dlt.readStream(f"{catalog}.{silver_schema}.claim")
    # Read the cleaned customer records
    customer = dlt.readStream(f"{catalog}.{silver_schema}.customer") 
    claim_policy = claim.join(policy, "policy_no")
    return claim_policy.join(customer, claim_policy.cust_id == customer.customer_id)

# --- CLAIM-POLICY-TELEMATICS ---
@dlt.table(
    name=f"{catalog}.{gold_schema}.customer_claim_policy_telematics",
    comment="claims with geolocation latitude/longitude",
        table_properties={
        "quality": "gold"
    }
)
def customer_claim_policy_telematics():
  telematics = dlt.read(f"{catalog}.{gold_schema}.aggregated_telematics")
  customer_claim_policy = dlt.read(f"{catalog}.{gold_schema}.customer_claim_policy").where("BOROUGH is not null")
  return (customer_claim_policy
            .withColumn("lat_long", get_lat_long(col("address")))
            .join(telematics, on="chassis_no")
        )
  


