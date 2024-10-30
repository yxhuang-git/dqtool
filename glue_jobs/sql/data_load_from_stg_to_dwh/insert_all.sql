insert into ${schemaTo}.${dwh_table}
    select * from ${schemaFrom}.${stg_table};