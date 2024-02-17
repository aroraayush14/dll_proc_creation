CREATE PROCEDURE [trans_product_gsdb_gsdb].[market_product_relationship_proc]   
 @pipeline_name [VARCHAR](100)
,@pipeline_run_id [VARCHAR](100)
,@pipeline_trigger_name [VARCHAR](100)
,@pipeline_trigger_id [VARCHAR](100)
,@pipeline_trigger_type [VARCHAR](100)
,@pipeline_trigger_date_time_utc [DATETIME2] 
AS
BEGIN TRY
--LOAD-TYPE: Incremental temp2trans 
WITH gen_hashkey as (
    SELECT
         CAST([product_key] AS [decimal](38,0)) AS [product_key],
        [market_product_code] AS [market_product_code],
        NULLIF([primary_flag],'') AS [primary_flag],
        [create_id] AS [create_id],
        CAST(STUFF(STUFF(STUFF(create_timestamp,13,0,':'),11,0,':'),9,0,' ') as DATETIME2(7)) as [create_timestamp],
        CAST([last_update_id] AS [varchar](30)) AS [last_update_id],
        CAST(STUFF(STUFF(STUFF(last_update_timestamp,13,0,':'),11,0,':'),9,0,' ') as DATETIME2(7)) AS [last_update_timestamp],
        CAST(STUFF(STUFF(STUFF(replace(ingest_partition,'-',''),13,0,':'),11,0,':'),9,0,' ') as DATETIME2(7)) as [ingest_partition],
        [ingest_channel] as [ingest_channel],
        [file_path] as [file_path],
        [root_path] as [root_path],
		[infa_operation_type] as [infa_operation_type],
		cast (STUFF(STUFF(STUFF(SUBSTRING(infa_operation_time,1,14),13,0,':'),11,0,':'),9,0,' ') as DATETIME2(7)) as [infa_operation_time],
        [infa_sortable_sequence] as [infa_sortable_sequence],
        CAST(hashbytes('sha2_256', concat(upper(trim(coalesce([product_key], '')))
		,'||',upper(trim(coalesce([market_product_code], ''))))) as VARBINARY(32)) AS [hash_key] 
    FROM    [trans_product_gsdb_gsdb].[market_product_relationship_temp]
)
,rn as (
    SELECT  *, ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY 
                 last_update_timestamp DESC,
				  infa_operation_time DESC,
                infa_sortable_sequence  DESC
        ) as _ELT_ROWNUMBERED
    FROM    gen_hashkey
),
data as (
    SELECT  *
    FROM    rn
    WHERE _ELT_ROWNUMBERED = 1
)
MERGE INTO    [trans_product_gsdb_gsdb].[market_product_relationship] tgt
USING (
    SELECT  *
    FROM    data
) src
ON ( src.[hash_key] = tgt.[hash_key] )
WHEN MATCHED THEN 
UPDATE SET
        tgt.[product_key] = src.[product_key],
        tgt.[market_product_code] = src.[market_product_code],
        tgt.[primary_flag] = src.[primary_flag],
        tgt.[create_id] = src.[create_id],
        tgt.[create_timestamp] = src.[create_timestamp],
        tgt.[last_update_id] = src.[last_update_id],
        tgt.[last_update_timestamp] = src.[last_update_timestamp],
        tgt.[ingest_partition] = src.[ingest_partition],
        tgt.[ingest_channel] = src.[ingest_channel],
        tgt.[file_path] = src.[file_path],
        tgt.[root_path] = src.[root_path],
        tgt.[trans_load_date_time_utc] = GETDATE(),
        tgt.[adle_transaction_code] = src.[infa_operation_type],
		tgt.[infa_operation_time]=src.[infa_operation_time],
	    tgt.[infa_sortable_sequence]=src.[infa_sortable_sequence],
        tgt.[pipeline_name] = @pipeline_name,
        tgt.[pipeline_run_id] = @pipeline_run_id,
        tgt.[pipeline_trigger_name] = @pipeline_trigger_name,
        tgt.[pipeline_trigger_id] = @pipeline_trigger_id,
        tgt.[pipeline_trigger_type] = @pipeline_trigger_type,
        tgt.[pipeline_trigger_date_time_utc] = @pipeline_trigger_date_time_utc
WHEN NOT MATCHED THEN 
    INSERT (
        [product_key],
        [market_product_code],
        [primary_flag],
        [create_id],
        [create_timestamp],
        [last_update_id],
        [last_update_timestamp],
        [ingest_partition],
        [ingest_channel],
        [file_path],
        [root_path],       
        [pipeline_name],
        [pipeline_run_id],
        [pipeline_trigger_name],
        [pipeline_trigger_id],
        [pipeline_trigger_type],
        [pipeline_trigger_date_time_utc],
		[trans_load_date_time_utc],
        [adle_transaction_code],
		[infa_operation_time],
	    [infa_sortable_sequence],
		[hash_key]		
    )
    VALUES (
        [src].[product_key],
        [src].[market_product_code],
        [src].[primary_flag],
        [src].[create_id],
        [src].[create_timestamp],
        [src].[last_update_id],
        [src].[last_update_timestamp],        
        [src].[ingest_partition],
        [src].[ingest_channel],
        [src].[file_path],
        [src].[root_path],        
        @pipeline_name,
        @pipeline_run_id,
        @pipeline_trigger_name,
        @pipeline_trigger_id,
        @pipeline_trigger_type,
        @pipeline_trigger_date_time_utc,
		GETDATE(),
        [src].[infa_operation_type],
		[src].[infa_operation_time],
	    [src].[infa_sortable_sequence],
		[src].[hash_key]
    );

 

END TRY
BEGIN CATCH
    DECLARE @db_name VARCHAR(200),
        @schema_name VARCHAR(200),
        @error_nbr INT,
        @error_severity INT,
        @error_state INT,
        @stored_proc_name VARCHAR(200),
        @error_message VARCHAR(8000),
        @created_date_time DATETIME2

    SET @db_name=DB_NAME()
    SET @schema_name=SUBSTRING (@pipeline_name, CHARINDEX('2', @pipeline_name) + 1, LEN(@pipeline_name) - CHARINDEX('2', @pipeline_name) - 3 )
    SET @error_nbr=ERROR_NUMBER()
    SET @error_severity=ERROR_SEVERITY()
    SET @error_state=ERROR_STATE()
    SET @stored_proc_name=ERROR_PROCEDURE()
    SET @error_message=ERROR_MESSAGE()
    SET @created_date_time=GETDATE()

    EXECUTE [adle_platform_orchestration].[elt_error_log_proc]
        @db_name,
        'ERROR',
        @schema_name,
        @error_nbr,
        @error_severity,
        @error_state,
        @stored_proc_name,
        'PROC',
        @error_message,
        @created_date_time,
        @pipeline_name,
        @pipeline_run_id,
        @pipeline_trigger_name,
        @pipeline_trigger_id,
        @pipeline_trigger_type,
        @pipeline_trigger_date_time_utc
        ;
    THROW;
END CATCH

;
GO
