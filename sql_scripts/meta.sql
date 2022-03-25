merge into de1m.krlv_meta_all trg
using ( select 'bank' schema_name, 'cards' table_name, ( select max( update_dt ) from de1m.krlv_stg_cards ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'bank', 'cards', coalesce( src.max_update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' ) ));


merge into de1m.krlv_meta_all trg
using ( select 'bank' schema_name, 'accounts' table_name, ( select max( update_dt ) from de1m.krlv_stg_accounts ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'bank', 'accounts', coalesce( src.max_update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' ) ));


merge into de1m.krlv_meta_all trg
using ( select 'bank' schema_name, 'clients' table_name, ( select max( update_dt ) from de1m.krlv_stg_clients ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'bank', 'clients', coalesce( src.max_update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' ) ));


merge into de1m.krlv_meta_all trg
using ( select 'de1m' schema_name, 'blcklst' table_name, to_date( '[date]', 'DDMMYYYY' ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'de1m', 'blcklst', coalesce( src.max_update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' ) ));

