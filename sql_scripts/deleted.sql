insert into de1m.KRLV_DWH_DIM_CARDS_HIST ( card_num, account, effective_from_dt, effective_to_dt, deleted_flg  ) 
select
    tgt.card_num, 
    tgt.account, 
    current_date, 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'Y'
from de1m.KRLV_DWH_DIM_CARDS_HIST tgt
left join de1m.krlv_stg_cards_del stg
on ( stg.card_num = tgt.card_num  )
where stg.card_num is null and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N';

update de1m.KRLV_DWH_DIM_CARDS_HIST tgt
set effective_to_dt = current_date - interval '1' second
where tgt.card_num not in (select card_num from de1m.krlv_stg_cards_del)
and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
and tgt.deleted_flg = 'N';

 
insert into de1m.KRLV_DWH_DIM_TERMINALS_HIST ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from_dt, effective_to_dt, deleted_flg  ) 
select
    tgt.terminal_id, 
    tgt.terminal_type,
    tgt.terminal_city,
    tgt.terminal_address, 
    to_date( '[date]', 'DDMMYYYY' ), 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'Y'
from de1m.KRLV_DWH_DIM_TERMINALS_HIST tgt
left join de1m.krlv_stg_terminals_del stg
on ( stg.terminal_id = tgt.terminal_id  )
where stg.terminal_id is null and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N';

update de1m.KRLV_DWH_DIM_TERMINALS_HIST tgt
set effective_to_dt = to_date( '[date]', 'DDMMYYYY' ) - interval '1' second
where tgt.terminal_id not in (select terminal_id from de1m.krlv_stg_terminals_del)
and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
and tgt.deleted_flg = 'N';


insert into de1m.KRLV_DWH_DIM_ACCOUNTS_HIST ( account, valid_to, client, effective_from_dt, effective_to_dt, deleted_flg  ) 
select
    tgt.account, 
    tgt.valid_to,
    tgt.client, 
    current_date, 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'Y'
from de1m.KRLV_DWH_DIM_ACCOUNTS_HIST tgt
left join de1m.krlv_stg_accounts_del stg
on ( stg.account = tgt.account and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )
where stg.account is null and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N';

update de1m.KRLV_DWH_DIM_ACCOUNTS_HIST tgt
set effective_to_dt = current_date - interval '1' second
where tgt.account not in (select account from de1m.krlv_stg_accounts_del)
and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
and tgt.deleted_flg = 'N';


insert into de1m.KRLV_DWH_DIM_CLIENTS_HIST ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone,effective_from_dt, effective_to_dt, deleted_flg  ) 
select
    tgt.client_id, 
    tgt.last_name,
    tgt.first_name,
    tgt.patronymic,
    tgt.date_of_birth,
    tgt.passport_num,
    tgt.passport_valid_to,
    tgt.phone,
    current_date, 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'Y'
from de1m.KRLV_DWH_DIM_CLIENTS_HIST tgt
left join de1m.krlv_stg_clients_del stg
on ( stg.client_id = tgt.client_id )
where stg.client_id is null and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N';

update de1m.KRLV_DWH_DIM_CLIENTS_HIST tgt
set effective_to_dt = current_date - interval '1' second
where tgt.client_id not in (select client_id from de1m.krlv_stg_clients_del)
and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
and tgt.deleted_flg = 'N';