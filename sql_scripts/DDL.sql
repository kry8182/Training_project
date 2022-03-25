-- Предварительное создание пустых таблиц

-- Мета
create table de1m.krlv_meta_all(
    schema_name varchar2(30),
    table_name varchar2(30),
    max_update_dt date
);
-- STG
create table de1m.krlv_stg_cards as select * from bank.cards where 1=0;
create table de1m.krlv_stg_cards_del as select card_num from bank.cards where 1=0;
create table de1m.krlv_stg_accounts as select * from bank.accounts where 1=0;
create table de1m.krlv_stg_accounts_del as select account from bank.accounts where 1=0;
create table de1m.krlv_stg_clients as select * from bank.clients where 1=0;
create table de1m.krlv_stg_clients_del as select client_id from bank.clients where 1=0;

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
create table de1m.krlv_stg_terminals_del (
    terminal_id varchar2(20)
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
    entry_dt date,
    
);
create table DE1M.KRLV_DWH_FACT_TRANS ( 
    trans_id varchar2(20),
    trans_date date,
    card_num varchar2(20),
    oper_type varchar2(20),
    amt decimal,
    oper_result varchar2(20),
    terminal varchar2(20),
);

-- Отчет
create table DE1M.KRLV_REP_FRAUD (
event_dt date,
passport varchar2(20),
fio varchar2(125),
phone varchar2(20),
event_type varchar2 (10),
report_dt date
);