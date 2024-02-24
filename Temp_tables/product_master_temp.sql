CREATE TABLE trans_product_gsdb_gsdb.product_master_temp (
  [infa_operation_time] [varchar](35) NULL,
  [infa_operation_type] [varchar](1) NULL,
  [corporate_product_line_code] [varchar](4),
  [create_id] [varchar](30),
  [create_timestamp] [varchar](35),
  [future_corporation_code] [varchar](4),
  [generic_category_code] [varchar](2),
  [last_update_id] [varchar](30),
  [last_update_timestamp] [varchar](35),
  [originating_company_code] [varchar](10),
  [product_category_code] [varchar](2),
  [product_class_id] [varchar](4),
  [product_id] [varchar](30),
  [product_key] [decimal](38, 0),
  [product_type_key] [decimal](38, 0),
  [product_unformatted_id] [varchar](30),
  [sponsor_organization_key] [decimal](38, 0),
  [unspsc_code] [varchar](8),
  [ingest_partition] [varchar](100) NULL,
  [ingest_channel] [varchar](100) NULL,
  [file_path] [varchar](100) NULL,
  [root_path] [varchar](100) NULL
)
WITH
(
  DISTRIBUTION = ROUND_ROBIN,
  HEAP
)
