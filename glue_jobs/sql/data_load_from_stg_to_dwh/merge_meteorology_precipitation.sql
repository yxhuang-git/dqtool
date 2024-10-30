MERGE INTO ${schemaTo}.${dwh_table} AS target
USING ${schemaFrom}.${stg_table} AS source
ON target.OBSERVATORY_NO = source.OBSERVATORY_NO AND 
target.YEAR_NOW = source.YEAR_NOW AND 
target.MONTH_NOW = source.MONTH_NOW AND 
target.DAY_NOW = source.DAY_NOW AND 
target.HOUR_NOW = source.HOUR_NOW AND 
target.MINUTE_NOW = source.MINUTE_NOW
WHEN MATCHED THEN
    UPDATE SET 
            target.PREFECTURES = source.PREFECTURES, 
            target.PLACE = source.PLACE, 
            target.INTERNATIONAL_PRICE = source.INTERNATIONAL_PRICE,  
            target.VALUE_NOW = source.VALUE_NOW, 
            target.QUALITY_VALUE_NOW = source.QUALITY_VALUE_NOW, 
            target.MAX_VALUE_THE_DAY = source.MAX_VALUE_THE_DAY, 
            target.QUALITY_MAX_VALUE_THE_DAY = source.QUALITY_MAX_VALUE_THE_DAY, 
            target.HOUR_MAX_VALUE_THE_DAY_START = source.HOUR_MAX_VALUE_THE_DAY_START, 
            target.MINUTE_MAX_VALUE_THE_DAY_START = source.MINUTE_MAX_VALUE_THE_DAY_START, 
            target.QUALITY_MAX_VALUE_THE_DAY_START = source.QUALITY_MAX_VALUE_THE_DAY_START, 
            target.EXTREME_VALUE_UPDATE = source.EXTREME_VALUE_UPDATE,
            target.EXTREME_VALUE_LESS_TEN_YEARS_UPDATE = source.EXTREME_VALUE_LESS_TEN_YEARS_UPDATE,
            target.TOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.QUALITY_TOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.QUALITY_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.YEAR_TOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.YEAR_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.MONTH_TOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.MONTH_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.DAY_TOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.DAY_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.QUALITY_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.QUALITY_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.YEAR_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.YEAR_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.MONTH_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.MONTH_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.DAY_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE = source.DAY_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            target.YEAR_STATISTICS_START = source.YEAR_STATISTICS_START
WHEN NOT MATCHED THEN
    INSERT 
        (
            OBSERVATORY_NO, 
            PREFECTURES, 
            PLACE, 
            INTERNATIONAL_PRICE, 
            YEAR_NOW, 
            MONTH_NOW, 
            DAY_NOW, 
            HOUR_NOW, 
            MINUTE_NOW, 
            VALUE_NOW, 
            QUALITY_VALUE_NOW, 
            MAX_VALUE_THE_DAY, 
            QUALITY_MAX_VALUE_THE_DAY, 
            HOUR_MAX_VALUE_THE_DAY_START, 
            MINUTE_MAX_VALUE_THE_DAY_START, 
            QUALITY_MAX_VALUE_THE_DAY_START, 
            EXTREME_VALUE_UPDATE,
            EXTREME_VALUE_LESS_TEN_YEARS_UPDATE,
            TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            QUALITY_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            YEAR_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            MONTH_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            DAY_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            QUALITY_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            YEAR_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            MONTH_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            DAY_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            YEAR_STATISTICS_START
        )
    VALUES 
        (
            source.OBSERVATORY_NO, 
            source.PREFECTURES, 
            source.PLACE, 
            source.INTERNATIONAL_PRICE, 
            source.YEAR_NOW, 
            source.MONTH_NOW, 
            source.DAY_NOW, 
            source.HOUR_NOW, 
            source.MINUTE_NOW, 
            source.VALUE_NOW, 
            source.QUALITY_VALUE_NOW, 
            source.MAX_VALUE_THE_DAY, 
            source.QUALITY_MAX_VALUE_THE_DAY, 
            source.HOUR_MAX_VALUE_THE_DAY_START, 
            source.MINUTE_MAX_VALUE_THE_DAY_START, 
            source.QUALITY_MAX_VALUE_THE_DAY_START, 
            source.EXTREME_VALUE_UPDATE,
            source.EXTREME_VALUE_LESS_TEN_YEARS_UPDATE,
            source.TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.QUALITY_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.YEAR_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.MONTH_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.DAY_TOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.QUALITY_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.YEAR_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.MONTH_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.DAY_MONTHTOP1_VALUE_UNTIL_THE_DAY_BEFORE, 
            source.YEAR_STATISTICS_START
        );
