MERGE INTO ${schemaTo}.${dwh_table} AS target
USING ${schemaFrom}.${stg_table} AS source
ON target.PRODUCT_CATEGORY = source.PRODUCT_CATEGORY AND 
target.AREA = source.AREA AND 
target.YEAR_MONTH = source.YEAR_MONTH
WHEN MATCHED THEN
    UPDATE SET target.BUDGET = source.BUDGET
WHEN NOT MATCHED THEN
    INSERT (PRODUCT_CATEGORY, AREA, YEAR_MONTH, BUDGET)
    VALUES (source.PRODUCT_CATEGORY, source.AREA, source.YEAR_MONTH, source.BUDGET);
