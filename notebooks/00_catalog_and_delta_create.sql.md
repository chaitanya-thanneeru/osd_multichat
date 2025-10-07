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

-- bronze_dc_fulfillment
INSERT INTO dev_catalog.gen_ai_usecase_brl.bronze_dc_fulfillment VALUES
('frow-0001','ORD-1001','DEL-9001','ACCT-001','00010','MAT-100',10,10,timestamp('2025-09-20 08:00:00'),timestamp('2025-09-20 08:05:00'),'pick_user_a',timestamp('2025-09-20 09:00:00'),NULL,false,'TRACK-111',false,'{"source":"sap_export"}', current_timestamp()),
('frow-0002','ORD-1002','DEL-9002','ACCT-002','00020','MAT-200',2,1,timestamp('2025-09-21 09:10:00'),timestamp('2025-09-21 09:12:00'),'pick_user_b',timestamp('2025-09-21 10:00:00'),'Damaged packaging',false,'TRACK-222',false,'{"source":"sap_export"}', current_timestamp()),
('frow-0003','ORD-1003','DEL-9003','ACCT-003','00030','MAT-300',1,1,timestamp('2025-09-22 07:30:00'),timestamp('2025-09-22 07:35:00'),'pick_user_c',timestamp('2025-09-22 08:30:00'),NULL,false,'TRACK-333',false,'{"source":"sap_export"}', current_timestamp()),
('frow-0004','ORD-1004','DEL-9004','ACCT-004','00040','MAT-400',5,5,timestamp('2025-09-23 10:00:00'),timestamp('2025-09-23 10:10:00'),'pick_user_d',timestamp('2025-09-23 11:00:00'),'GI_REVERSAL',true,'TRACK-444',false,'{"source":"sap_export"}', current_timestamp());

-- bronze_fedex_tracking_info
INSERT INTO dev_catalog.gen_ai_usecase_brl.bronze_fedex_tracking_info VALUES
('tnuid-111','TRACK-111','FEDEX','DELIVERED','Delivered to recipient','Bengaluru','KA','IN',timestamp('2025-09-20 12:30:00'),timestamp('2025-09-19 08:00:00'),timestamp('2025-09-19 06:00:00'),array('/FileStore/tables/osd/images/ord1001_pod.jpg'),1.2,1,1,'John Doe','PRIORITY',false,'{"carrier_payload":"ok"}', current_timestamp()),
('tnuid-222','TRACK-222','FEDEX','EXCEPTION','Package damaged','Chennai','TN','IN',NULL,timestamp('2025-09-21 09:00:00'),timestamp('2025-09-21 07:00:00'),array('/FileStore/tables/osd/images/ord1002_damage.jpg'),0.8,1,0,NULL,'STANDARD',false,'{"carrier_payload":"damaged"}', current_timestamp()),
('tnuid-333','TRACK-333','FEDEX','IN_TRANSIT','In transit to destination','Hyderabad','TG','IN',NULL,timestamp('2025-09-22 07:45:00'),timestamp('2025-09-22 06:00:00'),array(),0.5,1,0,NULL,'STANDARD',false,'{"carrier_payload":"in_transit"}', current_timestamp()),
('tnuid-444','TRACK-444','FEDEX','DELIVERED','Delivered - GI Reversed','Mumbai','MH','IN',timestamp('2025-09-23 11:30:00'),timestamp('2025-09-23 09:00:00'),timestamp('2025-09-23 06:30:00'),array('/FileStore/tables/osd/images/ord1004_pod.jpg'),4.0,2,1,'Jane Smith','EXPRESS',false,'{"carrier_payload":"delivered"}', current_timestamp());

-- bronze_fedex_scan_events
INSERT INTO dev_catalog.gen_ai_usecase_brl.bronze_fedex_scan_events VALUES
('scan-1001-1','tnuid-111',timestamp('2025-09-19 06:00:00'),'PU','Picked up',named_struct('city','Bengaluru','state','KA','country','IN'),'{}', current_timestamp()),
('scan-1002-1','tnuid-222',timestamp('2025-09-21 07:00:00'),'DLV_ATTEMPT','Delivery attempt - left at guard',named_struct('city','Chennai','state','TN','country','IN'),'{}', current_timestamp());

-- case_source
INSERT INTO dev_catalog.gen_ai_usecase_brl.case_source VALUES
('CASE-0001','c-0001','ACCT-001','Ravi Kumar','ravi.k@example.com','+91-9000000001','Missing item','Customer claims 1 item missing','',NULL,false,2,'TRACK-111', current_timestamp(), '{}'),
('CASE-0002','c-0002','ACCT-002','Sita Sharma','sita.s@example.com','+91-9000000002','Damaged item','Item received with visible damage','',NULL,false,0,'TRACK-222', current_timestamp(), '{}'),
('CASE-0003','c-0003','ACCT-003','Amit Patel','amit.p@example.com','+91-9000000003','General query','Where is my order?','',NULL,false,NULL,'TRACK-333', current_timestamp(), '{}'),
('CASE-0004','c-0004','ACCT-004','Reena Singh','reena.s@example.com','+91-9000000004','Delivered but reversed','Customer says not received though carrier shows delivered','',NULL,false,0,'TRACK-444', current_timestamp(), '{}');

-- contentdistribution
INSERT INTO dev_catalog.gen_ai_usecase_brl.contentdistribution VALUES
('cd-1','c-0002','/FileStore/tables/osd/images/ord1002_damage.jpg','ord1002_damage.jpg','image/jpeg', current_timestamp(), '{}'),
('cd-2','c-0001','/FileStore/tables/osd/images/ord1001_customer_photo.jpg','ord1001_customer_photo.jpg','image/jpeg', current_timestamp(), '{}');
