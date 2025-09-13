CREATE DATABASE claims_dev
USE claims_dev
CREATE SCHEMA demo

CREATE TABLE demo.policy (
    policy_no           VARCHAR(50)    NOT NULL PRIMARY KEY,
    cust_id             VARCHAR(50)    NOT NULL,        
    policytype          VARCHAR(50),
    pol_issue_date      DATE,
    pol_eff_date        DATE,
    pol_expiry_date     DATE,
    make                VARCHAR(50),
    model               VARCHAR(50),
    model_year          INT,
    chassis_no          VARCHAR(50),
    use_of_vehicle      VARCHAR(100),
    product             VARCHAR(100),
    sum_insured         FLOAT,
    premium             FLOAT,
    deductable          INT
)

CREATE TABLE demo.claim (
    claim_no                        VARCHAR(50)    NOT NULL PRIMARY KEY,
    policy_no                       VARCHAR(50)    NOT NULL,   
    claim_date                      VARCHAR(20),
    months_as_customer              INT,
    injury                          BIGINT,
    property                        BIGINT,
    vehicle                         BIGINT,
    total                           BIGINT,
    collision_type                  VARCHAR(50),
    number_of_vehicles_involved     INT,
    driver_age                      FLOAT,
    insured_relationship            VARCHAR(50),
    license_issue_date              VARCHAR(20),
    incident_date                   VARCHAR(20),
    incident_hour                   INT,
    incident_type                   VARCHAR(50),
    incident_severity               VARCHAR(50),
    number_of_witnesses             INT,
    suspicious_activity             BIT
)

CREATE TABLE demo.customer (
    customer_id INT NOT NULL PRIMARY KEY,
    date_of_birth VARCHAR(100) NULL,
    borough VARCHAR(100) NULL,
    neighborhood VARCHAR(150) NULL,
    zip_code VARCHAR(10) NULL,
    name VARCHAR(255) NULL
)



-- 1. ENABLE CHANGE TRACKING ON DB --
ALTER DATABASE claims_dev SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)

-- 2. ENABLE CHANGE TRACKING ON TABLES --
ALTER TABLE demo.policy ENABLE CHANGE_TRACKING
ALTER TABLE demo.claim ENABLE CHANGE_TRACKING
ALTER TABLE demo.customer ENABLE CHANGE_TRACKING

-- 3. ENABLE CDC on DB --
EXEC msdb.dbo.rds_cdc_enable_db 'claims_dev'

-- 4. ENABLE CDC ON TABLES --
EXEC sys.sp_cdc_enable_table
@source_schema = N'demo',
@source_name   = N'policy',
@role_name     = NULL,
@supports_net_changes = 1


EXEC sys.sp_cdc_enable_table
@source_schema = N'demo',
@source_name   = N'claim',
@role_name     = NULL,
@supports_net_changes = 1


EXEC sys.sp_cdc_enable_table
@source_schema = N'demo',
@source_name   = N'customer',
@role_name     = NULL,
@supports_net_changes = 1

-- 4. Set up DDL capture and schema evolution -
--> Run Script -> ddl_support_objects.sql which you can find here: https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-cdc <--