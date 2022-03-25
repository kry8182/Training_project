merge into DE1M.KRLV_DWH_DIM_CARDS_HIST tgt
using de1m.krlv_stg_cards stg
on( stg.card_num = tgt.card_num and deleted_flg = 'N' )
when matched then 
    update set tgt.effective_to_dt = stg.update_dt - interval '1' second
    where 1=1
    and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and (1=0
    or stg.account <> tgt.account or ( stg.account is null and tgt.account is not null ) or ( stg.account is not null and tgt.account is null )
    )
when not matched then 
    insert ( card_num, account, effective_from_dt, effective_to_dt, deleted_flg  ) 
    values ( stg.card_num, stg.account, stg.update_dt, to_date( '31.12.9999', 'DD.MM.YYYY' ), 'N' );

insert into DE1M.KRLV_DWH_DIM_CARDS_HIST ( card_num, account, effective_from_dt, effective_to_dt, deleted_flg  ) 
select
    stg.card_num,  
    stg.account, 
    stg.update_dt, 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
from DE1M.KRLV_DWH_DIM_CARDS_HIST tgt
inner join de1m.krlv_stg_cards stg
on ( stg.card_num = tgt.card_num and (tgt.effective_to_dt = stg.update_dt - interval '1' second) and deleted_flg = 'N' )
where 1=0
    or stg.account <> tgt.account or ( stg.account is null and tgt.account is not null ) or ( stg.account is not null and tgt.account is null );


merge into DE1M.KRLV_DWH_DIM_ACCOUNTS_HIST tgt
using de1m.krlv_stg_accounts stg
on( stg.account = tgt.account and deleted_flg = 'N' )
when matched then 
    update set tgt.effective_to_dt = stg.update_dt - interval '1' second
    where 1=1
    and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and (1=0
    or stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )   
    or stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null )
    )
when not matched then 
    insert ( account, valid_to, client, effective_from_dt, effective_to_dt, deleted_flg  ) 
    values ( stg.account, stg.valid_to, stg.client, stg.update_dt, to_date( '31.12.9999', 'DD.MM.YYYY' ), 'N' );

insert into DE1M.KRLV_DWH_DIM_ACCOUNTS_HIST ( account, valid_to, client, effective_from_dt, effective_to_dt, deleted_flg  ) 
select
    stg.account,   
    stg.valid_to,
    stg.client, 
    stg.update_dt, 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
from DE1M.KRLV_DWH_DIM_ACCOUNTS_HIST tgt
inner join de1m.krlv_stg_accounts stg
on ( stg.account = tgt.account and (tgt.effective_to_dt = stg.update_dt - interval '1' second) and deleted_flg = 'N' )
where 1=0
    or stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )   
    or stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null );

   
merge into DE1M.KRLV_DWH_DIM_CLIENTS_HIST tgt
using de1m.krlv_stg_clients stg
on( stg.client_id = tgt.client_id and deleted_flg = 'N' )
when matched then 
    update set tgt.effective_to_dt = stg.update_dt - interval '1' second
    where 1=1
    and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and (1=0
    or stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null )   
    or stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
    or stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null )
    or stg.date_of_birth <> tgt.date_of_birth or ( stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null )
    or stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null )
    or stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
    or stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null )
    )
when not matched then 
    insert ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from_dt, effective_to_dt, deleted_flg  ) 
    values ( stg.client_id, stg.last_name, stg.first_name, stg.patronymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, stg.update_dt, to_date( '31.12.9999', 'DD.MM.YYYY' ), 'N' );

insert into DE1M.KRLV_DWH_DIM_CLIENTS_HIST ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from_dt, effective_to_dt, deleted_flg  ) 
select
    stg.client_id,  
    stg.last_name,
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    stg.update_dt,
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
from DE1M.KRLV_DWH_DIM_CLIENTS_HIST tgt
inner join de1m.krlv_stg_clients stg
on ( stg.client_id = tgt.client_id and (tgt.effective_to_dt = stg.update_dt - interval '1' second) and deleted_flg = 'N' )
where 1=0
    or stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null )   
    or stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
    or stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null )
    or stg.date_of_birth <> tgt.date_of_birth or ( stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null )
    or stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null )
    or stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
    or stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null );


merge into DE1M.KRLV_DWH_DIM_TERMINALS_HIST tgt
using de1m.krlv_stg_terminals stg
on( stg.terminal_id = tgt.terminal_id and deleted_flg = 'N' )
when matched then 
    update set tgt.effective_to_dt = to_date( '[date]', 'DDMMYYYY' ) - interval '1' second
    where 1=1
    and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and (1=0
    or stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )
    or stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )
    or stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )
    )
when not matched then 
    insert ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from_dt, effective_to_dt, deleted_flg  ) 
    values ( stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address, to_date( '[date]', 'DDMMYYYY' ), to_date( '31.12.9999', 'DD.MM.YYYY' ), 'N' );

insert into DE1M.KRLV_DWH_DIM_TERMINALS_HIST ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from_dt, effective_to_dt, deleted_flg )
select
    stg.terminal_id,
    stg.terminal_type,  
    stg.terminal_city, 
    stg.terminal_address,
    to_date( '[date]', 'DDMMYYYY' ), 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
from DE1M.KRLV_DWH_DIM_TERMINALS_HIST tgt
inner join de1m.krlv_stg_terminals stg
on ( stg.terminal_id = tgt.terminal_id and (tgt.effective_to_dt = to_date( '[date]', 'DDMMYYYY' )  - interval '1' second) and deleted_flg = 'N' )
where 1=0
    or stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )
    or stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )
    or stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null );



insert into DE1M.KRLV_DWH_FACT_BLCKLST ( passport_num, entry_dt ) 
select passport_num, entry_dt from de1m.krlv_stg_blcklst
where entry_dt > coalesce( ( 
    select max_update_dt
    from de1m.krlv_meta_all
    where schema_name = 'de1m' and table_name = 'blcklst'
), to_date( '1900.01.01', 'YYYY.MM.DD' )); 


insert into DE1M.KRLV_DWH_FACT_TRANS ( trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal ) 
select trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal  from de1m.krlv_stg_transactions;
