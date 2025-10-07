-- 1. Consolidated Gold Table
CREATE OR REPLACE TABLE dev_catalog.gen_ai_usecase_brl.osd_consolidated_cases AS
SELECT
  c.case_number,
  c.Id AS case_id,
  c.Account_Number__c AS account_number,
  c.contact_name,
  c.contact_email,
  c.contact_phone,
  c.subject,
  c.description,
  c.case_solution,
  c.closed_date,
  c.is_closed,
  c.age_days,
  c.reported_received_qty,
  f.row_id AS dc_fulfillment_row_id,
  f.Order_Num,
  f.Delivery_Num,
  f.Customer_ID,
  f.tracking_number,
  f.Order_Qty,
  f.Pick_Qty,
  f.Pick_Start_time,
  f.Pick_End_time,
  f.HU_Audit_User AS pick_user,
  f.PGI_Date,
  f.RejectionReason,
  f.GI_Reversal_Flag,
  t.tracking_number_unique_id,
  t.latest_status_code,
  t.latest_status_description,
  t.actual_delivery_date,
  t.package_weight_kg,
  t.package_count,
  t.available_images,
  (SELECT collect_list(struct(event_time, event_code, event_description))
     FROM dev_catalog.gen_ai_usecase_brl.bronze_fedex_scan_events e
     WHERE e.tracking_number_unique_id = t.tracking_number_unique_id
     ORDER BY e.event_time) AS scan_timeline,
  (SELECT collect_list(cd.ContentDownloadUrl)
     FROM dev_catalog.gen_ai_usecase_brl.contentdistribution cd
     WHERE cd.LinkedEntityId = c.Id) AS attachments,
  current_timestamp() AS consolidated_at
FROM dev_catalog.gen_ai_usecase_brl.case_source c
LEFT JOIN dev_catalog.gen_ai_usecase_brl.bronze_dc_fulfillment f
  ON c.Account_Number__c = f.Customer_ID
     AND (c.linked_tracking_number = f.tracking_number OR c.linked_tracking_number IS NULL)
LEFT JOIN dev_catalog.gen_ai_usecase_brl.bronze_fedex_tracking_info t
  ON f.tracking_number = t.tracking_number;

-- 2. Integrity Check View
CREATE OR REPLACE VIEW dev_catalog.gen_ai_usecase_brl.osd_integrity_check AS
SELECT
  o.case_number,
  o.case_id,
  o.Order_Num,
  o.tracking_number,
  o.tracking_number_unique_id,
  CASE WHEN o.tracking_number IS NULL OR o.tracking_number = '' THEN false ELSE true END AS has_tracking_number,
  CASE WHEN o.tracking_number_unique_id IS NULL THEN false
       WHEN EXISTS (
          SELECT 1
          FROM dev_catalog.gen_ai_usecase_brl.bronze_fedex_tracking_info t
          WHERE t.tracking_number_unique_id = o.tracking_number_unique_id
            AND t.isdeleted = false
       ) THEN true ELSE false END AS tracking_not_deleted,
  CASE
    WHEN upper(coalesce(o.latest_status_code,'')) LIKE '%DELIVERED%' THEN 'DELIVERED'
    WHEN upper(coalesce(o.latest_status_code,'')) LIKE '%IN_TRANSIT%' OR upper(coalesce(o.latest_status_code,'')) LIKE '%TRANSIT%' THEN 'IN_TRANSIT'
    WHEN upper(coalesce(o.latest_status_code,'')) LIKE '%EXCEPTION%' THEN 'EXCEPTION'
    WHEN upper(coalesce(o.latest_status_code,'')) LIKE '%PENDING%' THEN 'PENDING'
    ELSE 'UNKNOWN' END AS canonical_latest_status,
  CASE
    WHEN o.reported_received_qty IS NULL THEN 'MISSING_REPORTED_QTY'
    WHEN o.reported_received_qty <> o.Order_Qty AND o.reported_received_qty <> o.Pick_Qty THEN 'MISMATCH_REPORT_VS_ORDER_PICK'
    WHEN o.package_count IS NOT NULL AND o.reported_received_qty > o.package_count * 100 THEN 'HEURISTIC_PKG_COUNT_MISMATCH'
    ELSE 'QTY_OK' END AS qty_check_result,
  CASE WHEN o.actual_delivery_date IS NOT NULL AND
            (upper(coalesce(o.latest_status_code,'')) LIKE '%DELIVERED%') AND
            (coalesce(o.reported_received_qty, -1) = 0)
       THEN 'DISPUTE_POSSIBLE' ELSE 'NO_DISPUTE' END AS delivery_dispute_flag,
  CASE WHEN o.RejectionReason IS NOT NULL OR o.GI_Reversal_Flag = true THEN 'ESCALATE_HUMAN' ELSE 'NO_ESCALATION_FLAG' END AS escalation_flag,
  CASE WHEN o.tracking_number_unique_id IS NOT NULL AND o.dc_fulfillment_row_id IS NOT NULL THEN 'EVIDENCE_PTR_OK' ELSE 'EVIDENCE_PTR_MISSING' END AS evidence_pointer_check,
  CASE
    WHEN (tracking_not_deleted = false) THEN 'FAIL_TRACKING'
    WHEN (canonical_latest_status = 'UNKNOWN') THEN 'FAIL_STATUS_UNKNOWN'
    WHEN (qty_check_result != 'QTY_OK') THEN 'FAIL_QTY'
    WHEN (escalation_flag = 'ESCALATE_HUMAN') THEN 'MANUAL_ESCALATION'
    WHEN (evidence_pointer_check != 'EVIDENCE_PTR_OK') THEN 'FAIL_EVIDENCE_POINTER'
    WHEN (delivery_dispute_flag = 'DISPUTE_POSSIBLE') THEN 'MANUAL_REVIEW_DISPUTE'
    ELSE 'PASS_AUTOCREATE' END AS integrity_outcome,
  current_timestamp() AS checked_at
FROM dev_catalog.gen_ai_usecase_brl.osd_consolidated_cases o;
