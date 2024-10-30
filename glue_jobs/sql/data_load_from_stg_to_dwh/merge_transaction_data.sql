MERGE INTO ${schemaTo}.${dwh_table} AS target
USING ${schemaFrom}.${stg_table} AS source
ON target.ROW_ID = source.ROW_ID
WHEN MATCHED THEN
    UPDATE SET 
            target.ORDER_ID = source.ORDER_ID, 
            target.ORDER_DAY = source.ORDER_DAY, 
            target.SHIPPING_DAY = source.SHIPPING_DAY,  
            target.SHIPPING_MODE = source.SHIPPING_MODE, 
            target.CUSTOMER_ID = source.CUSTOMER_ID, 
            target.CUSTOMER_NAME = source.CUSTOMER_NAME, 
            target.CUSTOMER_TYPE = source.CUSTOMER_TYPE, 
            target.MUNICIPALITY = source.MUNICIPALITY, 
            target.PREFECTURE = source.PREFECTURE, 
            target.COUNTRY = source.COUNTRY, 
            target.AREA = source.AREA,
            target.PRODUCT_ID = source.PRODUCT_ID,
            target.CATEGORY = source.CATEGORY, 
            target.SUB_CATEGORY = source.SUB_CATEGORY, 
            target.PRODUCT_NAME = source.PRODUCT_NAME, 
            target.SALES = source.SALES, 
            target.QUANTITY = source.QUANTITY, 
            target.DISCOUNT_RATE = source.DISCOUNT_RATE, 
            target.PROFIT = source.PROFIT
WHEN NOT MATCHED THEN
    INSERT 
        (
            ROW_ID,
            ORDER_ID, 
            ORDER_DAY, 
            SHIPPING_DAY,  
            SHIPPING_MODE,
            CUSTOMER_ID,
            CUSTOMER_NAME,
            CUSTOMER_TYPE,
            MUNICIPALITY,
            PREFECTURE,
            COUNTRY,
            AREA,
            PRODUCT_ID,
            CATEGORY, 
            SUB_CATEGORY, 
            PRODUCT_NAME, 
            SALES, 
            QUANTITY, 
            DISCOUNT_RATE, 
            PROFIT
        )
    VALUES 
        (
            source.ROW_ID,
            source.ORDER_ID, 
            source.ORDER_DAY, 
            source.SHIPPING_DAY,  
            source.SHIPPING_MODE,
            source.CUSTOMER_ID,
            source.CUSTOMER_NAME,
            source.CUSTOMER_TYPE,
            source.MUNICIPALITY,
            source.PREFECTURE,
            source.COUNTRY,
            source.AREA,
            source.PRODUCT_ID,
            source.CATEGORY, 
            source.SUB_CATEGORY, 
            source.PRODUCT_NAME, 
            source.SALES, 
            source.QUANTITY, 
            source.DISCOUNT_RATE, 
            source.PROFIT
        );
