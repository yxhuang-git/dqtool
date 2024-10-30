MERGE INTO ${schemaTo}.${dwh_table} AS target
USING ${schemaFrom}.${stg_table} AS source
ON target.ORDER_ID = source.ORDER_ID
WHEN MATCHED THEN
    UPDATE SET target.RETURN = source.RETURN
WHEN NOT MATCHED THEN
    INSERT (ORDER_ID, RETURN)
    VALUES (source.ORDER_ID, source.RETURN);
