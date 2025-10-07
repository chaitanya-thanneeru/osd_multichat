-- Create schema
CREATE SCHEMA IF NOT EXISTS dev_catalog.gen_ai_usecase_brl;

-- Bronze: DC Fulfillment
CREATE TABLE IF NOT EXISTS dev_catalog.gen_ai_usecase_brl.bronze_dc_fulfillment (
  row_id STRING,
  Order_Num STRING,
  Delivery_Num STRING,
  Customer_ID STRING,
  POSNR STRING,
  Material_ID STRING,
  Order_Qty DOUBLE,
  Pick_Qty DOUBLE,
  Pick_Start_time TIMESTAMP,
  Pick_End_time TIMESTAMP,
  HU_Audit_User STRING,
  PGI_Date TIMESTAMP,
  RejectionReason STRING,
  GI_Reversal_Flag BOOLEAN,
  tracking_number STRING,
  isdeleted BOOLEAN,
  raw_payload STRING,
  ingestion_timestamp TIMESTAMP
) USING DELTA;

-- Bronze: FedEx Tracking Info
CREATE TABLE IF NOT EXISTS dev_catalog.gen_ai_usecase_brl.bronze_fedex_tracking_info (
  tracking_number_unique_id STRING,
  tracking_number STRING,
  carrier_code STRING,
  latest_status_code STRING,
  latest_status_description STRING,
  latest_status_city STRING,
  latest_status_state STRING,
  latest_status_country STRING,
  actual_delivery_date TIMESTAMP,
  actual_pickup_date TIMESTAMP,
  ship_date TIMESTAMP,
  available_images ARRAY<STRING>,
  package_weight_kg DOUBLE,
  package_count INT,
  delivery_attempts INT,
  delivery_received_by_name STRING,
  service_type STRING,
  isdeleted BOOLEAN,
  raw_payload STRING,
  ingestion_timestamp TIMESTAMP
) USING DELTA;

-- Bronze: FedEx Scan Events
CREATE TABLE IF NOT EXISTS dev_catalog.gen_ai_usecase_brl.bronze_fedex_scan_events (
  scan_event_id STRING,
  tracking_number_unique_id STRING,
  event_time TIMESTAMP,
  event_code STRING,
  event_description STRING,
  event_location STRUCT<city:STRING, state:STRING, country:STRING>,
  raw_payload STRING,
  ingestion_timestamp TIMESTAMP
) USING DELTA;

-- Case Source (Salesforce / ServiceNow)
CREATE TABLE IF NOT EXISTS dev_catalog.gen_ai_usecase_brl.case_source (
  case_number STRING,
  Id STRING,
  Account_Number__c STRING,
  contact_name STRING,
  contact_email STRING,
  contact_phone STRING,
  subject STRING,
  description STRING,
  case_solution STRING,
  closed_date TIMESTAMP,
  is_closed BOOLEAN,
  age_days INT,
  reported_received_qty DOUBLE,
  linked_tracking_number STRING,
  ingestion_timestamp TIMESTAMP,
  raw_payload STRING
) USING DELTA;

-- Content Distribution (attachments)
CREATE TABLE IF NOT EXISTS dev_catalog.gen_ai_usecase_brl.contentdistribution (
  id STRING,
  LinkedEntityId STRING,
  ContentDownloadUrl STRING,
  file_name STRING,
  file_type STRING,
  ingestion_timestamp TIMESTAMP,
  raw_payload STRING
) USING DELTA;

-- Audit & Manual Review Tables
CREATE TABLE IF NOT EXISTS dev_catalog.gen_ai_usecase_brl.osd_action_audit (
  audit_id STRING,
  case_number STRING,
  action STRING,
  source TEXT,
  result STRING,
  created_at TIMESTAMP,
  payload STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS dev_catalog.gen_ai_usecase_brl.osd_manual_review (
  case_number STRING,
  case_id STRING,
  integrity_outcome STRING,
  reason STRING,
  queued_at TIMESTAMP,
  raw_context STRING
) USING DELTA;
