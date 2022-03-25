insert into DE1M.KRLV_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
select 
    trn.trans_date, 
    cln.passport_num, 
    cln.first_name || ' '  || cln.patronymic || ' ' || cln.last_name,
    cln.phone,
    1,
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


insert into DE1M.KRLV_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
select 
    trn.trans_date, 
    cln.passport_num, 
    cln.first_name || ' ' || cln.patronymic || ' ' || cln.last_name,
    cln.phone,
    2,
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


insert into DE1M.KRLV_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
select 
    trans_date, 
    passport_num, 
    first_name || ' ' || patronymic || ' ' || last_name,
    phone,
    3,
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


insert into DE1M.KRLV_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
select 
    trans_date, 
    passport_num, 
    first_name || ' ' || patronymic || ' ' || last_name,
    phone,
    4,
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
