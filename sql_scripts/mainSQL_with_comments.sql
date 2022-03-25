-- Предварительное создание пустых таблиц (делается один раз, в питоновский скрипт не входит)
create table de1m.krlv_stg_cards as select * from bank.cards where 1=0;
create table de1m.krlv_stg_cards_del as select card_num from bank.cards where 1=0;
create table de1m.krlv_stg_accounts as select * from bank.accounts where 1=0;
create table de1m.krlv_stg_accounts_del as select account from bank.accounts where 1=0;
create table de1m.krlv_stg_clients as select * from bank.clients where 1=0;
create table de1m.krlv_stg_clients_del as select client_id from bank.clients where 1=0;

create table de1m.krlv_meta_all(
    schema_name varchar2(30),
    table_name varchar2(30),
    max_update_dt date
);
create table de1m.krlv_stg_blcklst (
    passport_num varchar2(20),
    entry_dt date
);
create table de1m.krlv_stg_transactions (
    trans_id varchar2(20),
    trans_date date,
    card_num varchar2(20),
    oper_type varchar2(20),
    amt decimal(12,2),
    oper_result varchar2(20),
    terminal varchar2(20)
);
create table de1m.krlv_stg_terminals (
    terminal_id varchar2(20),
    terminal_type varchar2(20),
    terminal_city varchar2(40),
    terminal_address varchar2(100)
);

-- Таргеты
create table DE1M.KRLV_DWH_DIM_CARDS_HIST ( 
    card_num varchar2(30),
    account varchar2(30), 
    effective_from_dt date,
    effective_to_dt date,
    deleted_flg char(1) 
);
create table DE1M.KRLV_DWH_DIM_ACCOUNTS_HIST ( 
    client varchar2(30),
    account varchar2(30),
    valid_to date, 
    effective_from_dt date,
    effective_to_dt date,
    deleted_flg char(1) 
);
create table DE1M.KRLV_DWH_DIM_CLIENTS_HIST ( 
    client_id varchar2(20),
    last_name varchar2(40),
    first_name varchar2(40),
    patronymic varchar2(40),
    date_of_birth date,
    passport_num varchar2(20),
    passport_valid_to date,
    phone varchar2(20),
    effective_from_dt date,
    effective_to_dt date,
    deleted_flg char(1) 
);
create table DE1M.KRLV_DWH_DIM_TERMINALS_HIST (  
    terminal_id varchar2(20),
    terminal_type varchar2(20),
    terminal_city varchar2(40),
    terminal_address varchar2(100),
    effective_from_dt date,
    effective_to_dt date,
    deleted_flg char(1) 
);
create table DE1M.KRLV_DWH_FACT_BLCKLST ( 
    passport_num varchar2(20),
    entry_dt date 
);
create table DE1M.KRLV_DWH_FACT_TRANS ( 
    trans_id varchar2(20),
    trans_date date,
    card_num varchar2(20),
    oper_type varchar2(20),
    amt decimal,
    oper_result varchar2(20),
    terminal varchar2(20)
);
--Отчет
create table DE1M.KRLV_REP_FRAUD (
event_dt date,
passport varchar2(20),
fio varchar2(125),
phone varchar2(20),
event_type varchar2 (10),
report_dt date
);

-- ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА
-- 1 = Загрузка в STG

--Очистка прошлого STG
truncate table de1m.krlv_stg_cards;
truncate table de1m.krlv_stg_cards_del;
truncate table de1m.krlv_stg_accounts;
truncate table de1m.krlv_stg_accounts_del;
truncate table de1m.krlv_stg_clients;
truncate table de1m.krlv_stg_clients_del;
truncate table de1m.krlv_stg_blcklst;
truncate table de1m.krlv_stg_transactions;
truncate table de1m.krlv_stg_terminals;
truncate table de1m.krlv_stg_terminals_del;


-- В этом месте питон загружает данные из файлов, и далее опять работают sql-скрипты

-- 1.cards:
insert into de1m.krlv_stg_cards ( card_num, account, create_dt, update_dt )
select card_num, account, create_dt, coalesce (create_dt,update_dt) from bank.cards
where coalesce (create_dt,update_dt) > coalesce( ( 
    select max_update_dt
    from de1m.krlv_meta_all
    where schema_name = 'bank' and table_name = 'cards'
), to_date( '1800.01.01', 'YYYY.MM.DD' ));

insert into de1m.krlv_stg_cards_del ( card_num )
select card_num from bank.cards;

-- 1.accounts:
insert into de1m.krlv_stg_accounts ( account, valid_to, client, create_dt, update_dt )
select account, valid_to, client, create_dt, coalesce (create_dt,update_dt) from bank.accounts
where coalesce (create_dt,update_dt) > coalesce( ( 
    select max_update_dt
    from de1m.krlv_meta_all
    where schema_name = 'bank' and table_name = 'accounts'
), to_date( '1800.01.01', 'YYYY.MM.DD' ));

insert into de1m.krlv_stg_accounts_del ( account )
select account from bank.accounts;

-- 1.clients:
insert into de1m.krlv_stg_clients ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt )
select client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, coalesce (create_dt,update_dt) from bank.clients
where coalesce (create_dt,update_dt) > coalesce( ( 
    select max_update_dt
    from de1m.krlv_meta_all
    where schema_name = 'bank' and table_name = 'clients'
), to_date( '1800.01.01', 'YYYY.MM.DD' ));

insert into de1m.krlv_stg_clients_del ( client_id )
select client_id from bank.clients;

-- 1.terminals
insert into de1m.krlv_stg_terminals_del ( terminal_id )
select terminal_id from de1m.krlv_stg_terminals;

-- 2. Выделение вставок и изменений (transform) и вставка в их приемник (load)

-- 2.cards
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

-- 2.accounts 
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

-- 2.clients   
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

-- 2.terminals
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

-- 2. black_lists

insert into DE1M.KRLV_DWH_FACT_BLCKLST ( passport_num, entry_dt ) 
select passport_num, entry_dt from de1m.krlv_stg_blcklst
where entry_dt > coalesce( ( 
    select max_update_dt
    from de1m.krlv_meta_all
    where schema_name = 'de1m' and table_name = 'blcklst'
), to_date( '1900.01.01', 'YYYY.MM.DD' )); 

-- 2. transactions
insert into DE1M.KRLV_DWH_FACT_TRANS ( trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal ) 
select trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal  from de1m.krlv_stg_transactions;

-- 3 = Обработка удалений.
--3. cards
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

--3. terminals 
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

--3. accounts
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

--3. clients
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

-- 4 = Обновление метаданных.
-- 4. cards
merge into de1m.krlv_meta_all trg
using ( select 'bank' schema_name, 'cards' table_name, ( select max( update_dt ) from de1m.krlv_stg_cards ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'bank', 'cards', coalesce( src.max_update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' ) ));

-- 4. accounts
merge into de1m.krlv_meta_all trg
using ( select 'bank' schema_name, 'accounts' table_name, ( select max( update_dt ) from de1m.krlv_stg_accounts ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'bank', 'accounts', coalesce( src.max_update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' ) ));

-- 4. clients
merge into de1m.krlv_meta_all trg
using ( select 'bank' schema_name, 'clients' table_name, ( select max( update_dt ) from de1m.krlv_stg_clients ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'bank', 'clients', coalesce( src.max_update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' ) ));

-- 4. blacklist (Вместо [date] питон вставит дату из названия файла)
merge into de1m.krlv_meta_all trg
using ( select 'de1m' schema_name, 'blcklst' table_name, to_date( '[date]', 'DDMMYYYY' ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'de1m', 'blcklst', coalesce( src.max_update_dt, to_date( '1900.01.01', 'YYYY.MM.DD' ) ));


commit;
--=========================================================================================
-- 2-й раздел = Построение отчета (вместо [date] питон подставит дату из файла)

--2.1. Совершение операции при просроченном или заблокированном паспорте.
insert into DE1M.KRLV_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
select 
    trn.trans_date, 
    cln.passport_num, 
    cln.first_name || ' '  || cln.patronymic || ' ' || cln.last_name,
    cln.phone,
    '#1',
    TO_DATE('[date]', 'DDMMYYYY')
from 
DE1M.KRLV_DWH_FACT_TRANS trn
inner JOIN
DE1M.KRLV_DWH_DIM_CARDS_HIST crd
on trim(crd.card_num) = trim(trn.card_num) 
and ( TO_DATE('[date]', 'DDMMYYYY') between crd.effective_from_dt and crd.effective_to_dt) 
and crd.deleted_flg = 'N'
and trn.trans_date between TO_DATE('[date]', 'DDMMYYYY') and TO_DATE('[date]', 'DDMMYYYY') + interval '1' day
inner join
DE1M.KRLV_DWH_DIM_ACCOUNTS_HIST acc
on crd.account = acc.account
and ( TO_DATE('[date]', 'DDMMYYYY') between acc.effective_from_dt and acc.effective_to_dt)  
and acc.deleted_flg = 'N'
inner join
DE1M.KRLV_DWH_DIM_CLIENTS_HIST cln
on acc.client = cln.client_id
and ( TO_DATE('[date]', 'DDMMYYYY') between cln.effective_from_dt and cln.effective_to_dt) 
and cln.deleted_flg = 'N'
left join
DE1M.KRLV_DWH_FACT_BLCKLST blc
on cln.passport_num = blc.passport_num
where 1=0
or blc.passport_num is not null
or cln.passport_valid_to < TO_DATE('[date]', 'DDMMYYYY');

-- 2.2. Совершение операции при недействующем договоре.
insert into DE1M.KRLV_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
select 
    trn.trans_date, 
    cln.passport_num, 
    cln.first_name || ' ' || cln.patronymic || ' ' || cln.last_name,
    cln.phone,
    '#2',
    TO_DATE('[date]', 'DDMMYYYY')
from 
DE1M.KRLV_DWH_FACT_TRANS trn
inner JOIN
DE1M.KRLV_DWH_DIM_CARDS_HIST crd
on trim(crd.card_num) = trim(trn.card_num) 
and ( TO_DATE('[date]', 'DDMMYYYY') between crd.effective_from_dt and crd.effective_to_dt) 
and crd.deleted_flg = 'N'
and trn.trans_date between TO_DATE('[date]', 'DDMMYYYY') and TO_DATE('[date]', 'DDMMYYYY') + interval '1' day
inner join
DE1M.KRLV_DWH_DIM_ACCOUNTS_HIST acc
on crd.account = acc.account
and ( TO_DATE('[date]', 'DDMMYYYY') between acc.effective_from_dt and acc.effective_to_dt)  
and acc.deleted_flg = 'N'
inner join
DE1M.KRLV_DWH_DIM_CLIENTS_HIST cln
on acc.client = cln.client_id
and ( TO_DATE('[date]', 'DDMMYYYY') between cln.effective_from_dt and cln.effective_to_dt) 
and cln.deleted_flg = 'N'
where acc.valid_to < TO_DATE('[date]', 'DDMMYYYY');

-- 2.3. Совершение операций в разных городах в течение одного часа.
insert into DE1M.KRLV_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
select 
    trans_date, 
    passport_num, 
    first_name || ' ' || patronymic || ' ' || last_name,
    phone,
    '#3',
    TO_DATE('[date]', 'DDMMYYYY')
from (
    select 
        card_num,
        terminal_city,
        lag_city,
        trans_date,
        lag_date
    from (
            SELECT  
                card_num,
                trans_date,
                terminal_city,
                coalesce(LAG(trm.terminal_city) OVER (PARTITION BY trn.card_num ORDER BY trn.trans_date ASC), trm.terminal_city) lag_city,
                coalesce(LAG(trn.trans_date) OVER (PARTITION BY trn.card_num ORDER BY trn.trans_date ASC), trn.trans_date) lag_date
            FROM 
            DE1M.KRLV_DWH_FACT_TRANS trn
            inner join 
            DE1M.KRLV_DWH_DIM_TERMINALS_HIST trm
            ON trn.terminal = trm.terminal_id
            and ( TO_DATE('[date]', 'DDMMYYYY') between trm.effective_from_dt and trm.effective_to_dt) 
            and trm.deleted_flg = 'N'
            and trn.trans_date between ( TO_DATE('[date]', 'DDMMYYYY') - interval '1' hour ) and (TO_DATE('[date]', 'DDMMYYYY') + interval '1' day)
        ) t
    where terminal_city <> lag_city 
    and trans_date - interval '1' hour < lag_date) t1
inner join
DE1M.KRLV_DWH_DIM_CARDS_HIST crd
on trim(crd.card_num) = trim(t1.card_num) 
and ( TO_DATE('[date]', 'DDMMYYYY') between crd.effective_from_dt and crd.effective_to_dt) 
and crd.deleted_flg = 'N'
inner join
DE1M.KRLV_DWH_DIM_ACCOUNTS_HIST acc
on crd.account = acc.account
and ( TO_DATE('[date]', 'DDMMYYYY') between acc.effective_from_dt and acc.effective_to_dt)  
and acc.deleted_flg = 'N'
inner join
DE1M.KRLV_DWH_DIM_CLIENTS_HIST cln
on acc.client = cln.client_id
and ( TO_DATE('[date]', 'DDMMYYYY') between cln.effective_from_dt and cln.effective_to_dt) 
and cln.deleted_flg = 'N';


-- 2.4. Попытка подбора суммы.
insert into DE1M.KRLV_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
select 
    trans_date, 
    passport_num, 
    first_name || ' ' || patronymic || ' ' || last_name,
    phone,
    '#4',
    TO_DATE('[date]', 'DDMMYYYY')
from (
    select 
        card_num,
        amt,
        pre_amt,
        pre_2_amt,
        pre_3_amt,
        trans_date,
        first_date,
        oper_result,
        pre_result,
        pre_2_result,
        pre_3_result
    from (
            SELECT  
                card_num,
                amt,               
                coalesce(LAG(amt) OVER (PARTITION BY card_num ORDER BY trans_date ASC), amt) pre_amt,
                coalesce(LAG(amt, 2) OVER (PARTITION BY card_num ORDER BY trans_date ASC), amt) pre_2_amt,
                coalesce(LAG(amt, 3) OVER (PARTITION BY card_num ORDER BY trans_date ASC), amt) pre_3_amt,
                trans_date,
                coalesce(LAG(trans_date, 3) OVER (PARTITION BY card_num ORDER BY trans_date ASC), trans_date) first_date,
                oper_result,
                coalesce(LAG(oper_result) OVER (PARTITION BY card_num ORDER BY trans_date ASC), oper_result) pre_result,
                coalesce(LAG(oper_result, 2) OVER (PARTITION BY card_num ORDER BY trans_date ASC), oper_result) pre_2_result,
                coalesce(LAG(oper_result, 3) OVER (PARTITION BY card_num ORDER BY trans_date ASC), oper_result) pre_3_result
            FROM 
            DE1M.KRLV_DWH_FACT_TRANS
            where 1=1
            and trans_date between ( TO_DATE('[date]', 'DDMMYYYY') - interval '20' minute ) and (TO_DATE('[date]', 'DDMMYYYY') + interval '1' day)
            and ( oper_type in ( 'WITHDRAW', 'PAYMENT' ))
        ) t
    where 1=1
    and trans_date - interval '20' minute < first_date
    and oper_result = 'SUCCESS' and pre_result = 'REJECT' and pre_2_result = 'REJECT' and pre_3_result = 'REJECT'
    and amt < pre_amt  and  pre_amt < pre_2_amt and pre_2_amt < pre_3_amt
            ) t1
inner join
DE1M.KRLV_DWH_DIM_CARDS_HIST crd
on trim(crd.card_num) = trim(t1.card_num) 
and ( TO_DATE('[date]', 'DDMMYYYY') between crd.effective_from_dt and crd.effective_to_dt) 
and crd.deleted_flg = 'N'
inner join
DE1M.KRLV_DWH_DIM_ACCOUNTS_HIST acc
on crd.account = acc.account
and ( TO_DATE('[date]', 'DDMMYYYY') between acc.effective_from_dt and acc.effective_to_dt)  
and acc.deleted_flg = 'N'
inner join
DE1M.KRLV_DWH_DIM_CLIENTS_HIST cln
on acc.client = cln.client_id
and ( TO_DATE('[date]', 'DDMMYYYY') between cln.effective_from_dt and cln.effective_to_dt) 
and cln.deleted_flg = 'N';

commit;
