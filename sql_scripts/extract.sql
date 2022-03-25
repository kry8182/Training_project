insert into de1m.krlv_stg_cards ( card_num, account, create_dt, update_dt )
select card_num, account, create_dt, coalesce (create_dt,update_dt) from bank.cards
where coalesce (create_dt,update_dt) > coalesce( ( 
    select max_update_dt
    from de1m.krlv_meta_all
    where schema_name = 'bank' and table_name = 'cards'
), to_date( '1800.01.01', 'YYYY.MM.DD' ));

insert into de1m.krlv_stg_cards_del ( card_num )
select card_num from bank.cards;


insert into de1m.krlv_stg_accounts ( account, valid_to, client, create_dt, update_dt )
select account, valid_to, client, create_dt, coalesce (create_dt,update_dt) from bank.accounts
where coalesce (create_dt,update_dt) > coalesce( ( 
    select max_update_dt
    from de1m.krlv_meta_all
    where schema_name = 'bank' and table_name = 'accounts'
), to_date( '1800.01.01', 'YYYY.MM.DD' ));

insert into de1m.krlv_stg_accounts_del ( account )
select account from bank.accounts;


insert into de1m.krlv_stg_clients ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt )
select client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, coalesce (create_dt,update_dt) from bank.clients
where coalesce (create_dt,update_dt) > coalesce( ( 
    select max_update_dt
    from de1m.krlv_meta_all
    where schema_name = 'bank' and table_name = 'clients'
), to_date( '1800.01.01', 'YYYY.MM.DD' ));

insert into de1m.krlv_stg_clients_del ( client_id )
select client_id from bank.clients;


insert into de1m.krlv_stg_terminals_del ( terminal_id )
select terminal_id from de1m.krlv_stg_terminals;