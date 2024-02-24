CREATE TABLE trans_product_gsdb_gsdb.product_master (
  [create_timestamp] [varchar](35),
  [unspsc_code] [varchar](8),
  [last_update_id] [varchar](30),
  [last_update_timestamp] [varchar](35),
  [future_corporation_code] [varchar](4),
  [product_key] [decimal](38, 0),
  [product_type_key] [decimal](38, 0),
  [product_class_id] [varchar](4),
  [corporate_product_line_code] [varchar](4),
  [originating_company_code] [varchar](10),
  [generic_category_code] [varchar](2),
  [product_category_code] [varchar](2),
  [product_id] [varchar](30),
  [product_unformatted_id] [varchar](30),
  [sponsor_organization_key] [decimal](38, 0),
  [create_id] [varchar](30),
  [ingest_partition] [varchar](100) NULL,
  [ingest_channel] [varchar](100) NULL,
  [file_path] [varchar](100) NULL,
  [root_path] [varchar](100) NULL,
  [pipeline_name] [varchar](100) NULL,
  [pipeline_run_id] [varchar](100) NULL,
  [pipeline_trigger_name] [varchar](100) NULL,
  [pipeline_trigger_id] [varchar](100) NULL,
  [pipeline_trigger_type] [varchar](100) NULL,
  [pipeline_trigger_date_time_utc] [datetime2](7) NULL,
  [trans_load_date_time_utc] [datetime2](7) NULL,
  [adle_transaction_code] [char](1) NULL,
  [hash_key] [varbinary](32) NULL
)
WITH
(
  DISTRIBUTION = HASH([hash_key]),
  CLUSTERED COLUMNSTORE INDEX
)
GO;
