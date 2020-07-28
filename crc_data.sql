

create or replace procedure Csa_crc_etl_root_0(run_date varchar2) is
    --next_monthstr varchar2(50) := to_char(add_months(TO_DATE(str_date, 'dd/mm/yyyy'),1),'mm/yyyy');
BEGIN
  DECLARE
    sql_str varchar2(5000);
    table_name varchar2(150) := 'csa_card_temp_root_0'|| run_date;
    partition_value varchar2(50);
    par_month varchar(50) := to_char(to_date(run_date, 'ddmmyyyy'), 'yyyy/mm');
  BEGIN
    Drop_table_if_exists(table_name);
    select partition
    into partition_value
    from SDM_COL_BALANCE_CRC_partitions_view
    where month = par_month;

    sql_str := 'create table '||table_name||'
    as
    (select t.run_date,t.appl_id,t.last_stmt_date statement_date,
    t.out_principals POS, t.dpd crc_dpd, ceil(t.dpd/30) crc_bucket,
    a.total_amount_due, -- tien phai dong
    a.credit_limit, a.ending_balance,
    a.DUE_DATE

    from sdm.sdm_col_balance_crc partition('||partition_value||') t
    left join COL_TBL_MAI_STATEMENT_CRC_BK a --SDM.SDM_COL_STATEMENT_CRC a
         on t.appl_id = a.appl_id  and t.last_stmt_date = a.STATEMENT_DATE

    where t.run_date = TO_DATE('''||run_date||''', ''ddmmyyyy'')
    and t.dpd between 1 and 180
    and t.status = ''A''
    and t.out_principals > 0
    )';

    EXECUTE IMMEDIATE sql_str;
    commit;
  end;

end Csa_crc_etl_root_0;
/
--endstore
--====================================================================
-- Step1 main:
-- select appl_id pool
--====================================================================
create or replace procedure Csa_crc_etl_root(run_date varchar2) is
BEGIN
  DECLARE
    sql_str varchar2(5000);
    table_name varchar2(150) := 'csa_card_temp_root_' || run_date;
    table_s1 varchar2(150) := 'csa_card_temp_root_0'|| run_date;
  BEGIN
    Drop_table_if_exists(table_name);

    sql_str := 'create table '||table_name||'
    as
    select to_char(t.appl_id) appl_id  , t.run_date,t.statement_date, t.POS,
    t.total_amount_due, -- tien phai dong
    t.credit_limit,t.ending_balance, t.DUE_DATE,
    t.crc_dpd, t.crc_bucket

    from (select x.*, ROW_NUMBER() OVER (PARTITION BY x.Appl_id ORDER BY x.run_date desc) as cnt_row
                      from '||table_s1||' x )  t
    where t.cnt_row  = 1
    and t.total_amount_due > 0
    ';

    EXECUTE IMMEDIATE sql_str;
    commit;
  end;
end Csa_crc_etl_root;
/
--endstore

create or replace procedure Csa_crc_etl_everything(run_date varchar2) is
BEGIN
  DECLARE
    table_name varchar2(150) := 'csa_card_temp_every_' || run_date;
    table_root varchar2(150) := 'csa_card_temp_root_' || run_date;
  BEGIN
    Drop_table_if_exists(table_name);

    EXECUTE IMMEDIATE 'create table '||table_name||'
    as
    select a.appl_id, c.agreement_no, c.cus_id
    --ceil(MONTHS_BETWEEN(to_date('''||run_date||''', ''ddmmyyyy''), c.disbursal_date)) MOB
    from
    '||table_root||' a
    left join sdm.sdm_col_everything c on a.appl_id = c.agreement_id
    ';

  end;
end Csa_crc_etl_everything;
/
--endstore

create or replace procedure Csa_crc_etl_dpd(run_date varchar2, month_l number) is
--
    BEGIN
      DECLARE
        table_name varchar2(150) := 'csa_card_temp_dpd'||month_l||'_'||run_date;
        table_root varchar2(150) := 'csa_card_temp_root_'||run_date;
        par_month varchar(50) := to_char(add_months(to_date(run_date, 'ddmmyyyy'), - month_l), 'yyyy/mm');
        par_date varchar(50) := to_char(add_months(to_date(run_date, 'ddmmyyyy'), - month_l), 'ddmmyyyy');
        partition_value varchar(50);
      begin
        Drop_table_if_exists(table_name);
        Drop_table_if_exists(table_name||'_temp');
        select partition
        into partition_value
        from SDM_COL_BALANCE_CRC_partitions_view
        where month = par_month;

        -- make buffer table - posting date in 1 month
        EXECUTE IMMEDIATE  'create table '||table_name||'_temp as
        select t.appl_id, t.dpd crc_dpd_'||month_l||',
               ceil(t.dpd/30) crc_bucket_'||month_l||',
               a.total_amount_due  total_amount_due_'||month_l||',
               a.ending_balance ending_balance_'||month_l||'

        from sdm.sdm_col_balance_crc partition('||partition_value||') t
        left join COL_TBL_MAI_STATEMENT_CRC_BK a --SDM.SDM_COL_STATEMENT_CRC a
        on t.appl_id = a.appl_id  and t.last_stmt_date = a.STATEMENT_DATE

        where t.run_date = to_date('''||par_date||''', ''ddmmyyyy'')
        ';

        EXECUTE IMMEDIATE  'create table '||table_name||' as
        select *
        from '||table_name||'_temp t
        where exists( select 1 from  '||table_root||' x  where t.appl_id = x.appl_id)
        ';

        Drop_table_if_exists(table_name||'_temp');

     end;
end Csa_crc_etl_dpd;
/
--endstore

create or replace procedure Csa_crc_etl_label(run_date varchar2, run_mode varchar2) is
    BEGIN
      DECLARE
        table_name varchar2(150) := 'csa_card_temp_label_'||run_date;
        table_root varchar2(150) := 'csa_card_temp_root_'||run_date;
        par_month varchar(50) := to_char(add_months(to_date(run_date, 'ddmmyyyy'), 1), 'yyyy/mm');
        par_date varchar(50) := to_char(add_months(to_date(run_date, 'ddmmyyyy'), 1), 'ddmmyyyy');
        partition_value varchar(50);
      begin
        Drop_table_if_exists(table_name);
        Drop_table_if_exists(table_name||'_temp');

        if run_mode = 'history' then
            select partition
            into partition_value
            from SDM_COL_BALANCE_CRC_partitions_view
            where month = par_month;

            -- make buffer table - posting date in 1 month
            EXECUTE IMMEDIATE  'create table '||table_name||'_temp as
            select t.appl_id, ceil(t.dpd/30) crc_bucket_label
            from sdm.sdm_col_balance_crc partition('||partition_value||') t
            where t.run_date = to_date('''||par_date||''', ''ddmmyyyy'')
            ';

            EXECUTE IMMEDIATE  'create table '||table_name||' as
            select *
            from '||table_name||'_temp t
            where exists( select 1 from  '||table_root||' x  where t.appl_id = x.appl_id)
            ';

            Drop_table_if_exists(table_name||'_temp');
         else
             EXECUTE IMMEDIATE  'create table '||table_name||' as
             select appl_id, 0 as crc_bucket_label
             from  '||table_root;
         end if ;
     end;
end Csa_crc_etl_label;
/
--endstore
--====================================================================
-- transaction type and amount
--====================================================================
create or replace procedure Csa_crc_etl_transaction(run_date varchar2) is
-- Step2_60 : buffer table transaction 2 month
    BEGIN
      DECLARE
        table_tran varchar2(150) := 'csa_card_temp_transaction_'||run_date;
        table_root varchar2(150) := 'csa_card_temp_root_'||run_date;
      begin
        Drop_table_if_exists(table_tran);

        -- make buffer table - posting date in 1 month
        EXECUTE IMMEDIATE  'create table '||table_tran||' as
        select t.appl_id, t.amount, t.TRANSACTION_CODE, t.PLAN_DESC, t.posting_date
        from sdm.sdm_col_transaction_crc t
        where exists( select 1 from  '||table_root||' x  where t.appl_id = x.appl_id)
        and t.posting_date between add_months(to_date('''||run_date||''', ''ddmmyyyy''),-6)
                       and to_date('''||run_date||''', ''ddmmyyyy'')-1
        ';

    end;
end Csa_crc_etl_transaction;
/
--endstore
--====================================================================
-- Step2_63 : retail
--====================================================================
create or replace procedure Csa_crc_etl_tranRetail(run_date varchar2) is

    table_name varchar2(150) := 'csa_card_temp_tranRetail_' ||run_date;
    table_tran varchar2(150) := 'csa_card_temp_transaction_'||run_date;
  begin
    Drop_table_if_exists(table_name);
    EXECUTE IMMEDIATE 'create table '||table_name||' as
        select  a.appl_id, sum(a.amount) crc_total_retail,
        max(a.amount) max_retail, count(1) crc_count_retail,
        to_date('''||run_date||''',''ddmmYYYY'') - max(posting_date) as crc_last_retail
        from '||table_tran||' a
        where a.TRANSACTION_CODE in (
             0105, 2605, 0109, 2511, 00105, 02605, 00109, 02511,
             2139,2141,4123,2147,2149,4225,4227,4241,4243,0235,0239,4121,4127,4131,
             8669,8671,8677,8679,02139,0141,04123,02147,02149,04225,04227,04241,04243,
             00235,00239,04121,04127,04131,08669,08671,08677,08679
             )
        group by a.appl_id
        ';
end Csa_crc_etl_tranRetail;
/
--endstore

--====================================================================
-- Step2_63 : cash
--====================================================================
create or replace procedure Csa_crc_etl_tranCash(run_date varchar2) is
    table_name varchar2(150) := 'csa_card_temp_tranCash_' ||run_date;
    table_tran varchar2(150) := 'csa_card_temp_transaction_'||run_date;
  begin
    Drop_table_if_exists(table_name);
    EXECUTE IMMEDIATE 'create table '||table_name||' as
        select  a.appl_id, sum(a.amount) crc_total_cash,
        max(a.amount) crc_max_cash, count(1) crc_count_cash,
        to_date('''||run_date||''',''ddmmYYYY'') - max(posting_date) as crc_last_cash
        from '||table_tran||' a
        where a.TRANSACTION_CODE in (0104,0301,2513,00104,00301,02513,
                                    0241,4125,4129,00241,04125,04129)
        group by a.appl_id
        ';
end Csa_crc_etl_tranCash;
/
--endstore
--====================================================================
-- INSURANCE
--====================================================================
-- Step2_63 : INSURANCE
create or replace procedure Csa_crc_etl_insurance(run_date varchar2) is

    table_name varchar2(150) := 'csa_card_temp_insurance_' ||run_date;
    table_tran varchar2(150) := 'csa_card_temp_transaction_'||run_date;
  begin
    Drop_table_if_exists(table_name);
    EXECUTE IMMEDIATE 'create table '||table_name||' as
    with a as (
    select t.*,
    case when t.PLAN_DESC LIKE ''%INSURANCE%''
              and t.Transaction_Code not in (00567,00596,01007,01009)
         then 1 else 0 end as INSURANCE
    from '||table_tran||' t
    )
    select a.appl_id, sum(a.amount) crc_sum_insurance,  count(1) crc_count_insurance
    from a
    where a.INSURANCE = 1
    group by a.appl_id
    ';
end Csa_crc_etl_insurance;
/
--endstore
--=============================================
-- Step 3 : paid_count_60 day CRC
--  public table - 10
--=============================================
create or replace procedure Csa_crc_etl_paid(run_date varchar2) is
   table_name varchar2(150) := 'csa_card_temp_paid3m_'|| run_date;
   begin
     Drop_table_if_exists(table_name);
     Drop_table_if_exists(table_name||'_temp');

     -- make buffer table
     EXECUTE IMMEDIATE 'create table '||table_name||'_temp
     as
     select a.APPL_ID, a.amount, a.pay_date ,
     row_number() OVER (PARTITION BY a.APPL_ID ORDER BY a.pay_date DESC ) AS row_num
     from dwcrc.it_f1c_payment_details  a
     WHERE a.pay_date  between  add_months(to_date('''||run_date||''', ''ddmmyyyy''),-6)
           and  to_date('''||run_date||''',''ddmmYYYY'')-1
     ';

     -- group by data
     EXECUTE IMMEDIATE 'create table '|| table_name || '
     as
     with s1 as (
         select a.APPL_ID, count(1) as crc_paid_count ,
            sum(a.amount) as crc_total_paid,
            avg(a.amount) as crc_avg_paid,
            stddev(a.amount) as crc_std_paid,
            to_date('''||run_date||''',''ddmmYYYY'') - max(a.pay_date) as crc_last_paid
         from '||table_name||'_temp    a
         group by a.APPL_ID
     ),
     s2 as (
         select APPL_ID, amount
         from '||table_name||'_temp
         where row_num = 1 and amount > 0
     )
     select s1.*, s2.amount crc_last_paid_amount
     from s1 left join s2 on s1.appl_id = s2.appl_id
     ' ;

     Drop_table_if_exists(table_name||'_temp');


end Csa_crc_etl_paid;
/
--endstore

-- Step end :  Combime

create or replace procedure Csa_crc_etl_delete_tables(run_date varchar2) is

  begin
      declare
          type namesarray IS VARRAY(5) OF VARCHAR2(100);
          total integer;
          tablenames namesarray;
      begin
       tablenames := namesarray('csa_card_temp_root_', 'csa_card_temp_transaction_cash_',
                                'csa_card_temp_transaction_retail_', 'csa_card_temp_insurance_',
                                'csa_card_temp_paid60_');
       total := tablenames.count;

       for i in 1 .. total loop
           Drop_table_if_exists(tablenames(i)||run_date);
       end LOOP;
   end;
end Csa_crc_etl_delete_tables;
/
--endstore

create or replace procedure Csa_etl_delete_tables(run_date varchar2, prefix varchar2) is

   BEGIN
    DECLARE
       c_name all_tables.table_name%type;

       CURSOR c_all_tables is
          SELECT table_name FROM all_tables
          where owner = 'COMMON'
          and table_name like prefix||'%'||run_date;

    BEGIN
       OPEN c_all_tables;
       LOOP
       FETCH c_all_tables into c_name;
          EXIT WHEN c_all_tables%notfound;
          dbms_output.put_line( c_name );
          Drop_table_if_exists(c_name );

       END LOOP;
       CLOSE c_all_tables;
    END;
end Csa_etl_delete_tables;
/
--endstore

create or replace procedure Csa_crc_etl_combine(run_date varchar2) is
   table_name varchar2(150) := 'CSA_crc_etl_data_'||run_date;
   begin
   Drop_table_if_exists(table_name);

   EXECUTE IMMEDIATE '
   create table '||table_name||'  as
   select r.APPL_ID APPLID, agreement_no APPL_ID,
    to_char(STATEMENT_DATE, ''dd'') STATEMENT_DAY, to_char(DUE_DATE, ''dd'') DUE_DATE,
    POS, TOTAL_AMOUNT_DUE, CREDIT_LIMIT, ENDING_BALANCE, CRC_PAID_COUNT, CRC_TOTAL_PAID,
    CRC_AVG_PAID, CRC_STD_PAID, CRC_LAST_PAID, CRC_LAST_PAID_AMOUNT,
    CRC_SUM_INSURANCE, CRC_COUNT_INSURANCE, CRC_TOTAL_CASH, CRC_MAX_CASH,
    CRC_COUNT_CASH, CRC_LAST_CASH, CRC_TOTAL_RETAIL, MAX_RETAIL, CRC_COUNT_RETAIL,
    CRC_LAST_RETAIL, crc_dpd, crc_bucket,
    CRC_DPD_1, CRC_DPD_2, CRC_DPD_3,
    CRC_DPD_4, CRC_DPD_5, CRC_DPD_6,
    CRC_BUCKET_1, CRC_BUCKET_2,CRC_BUCKET_3,
    CRC_BUCKET_4, CRC_BUCKET_5,CRC_BUCKET_6,
    TOTAL_AMOUNT_DUE_1,TOTAL_AMOUNT_DUE_2,
    TOTAL_AMOUNT_DUE_3,TOTAL_AMOUNT_DUE_4,
    TOTAL_AMOUNT_DUE_5,TOTAL_AMOUNT_DUE_6,
    ENDING_BALANCE_1,ENDING_BALANCE_2,
    ENDING_BALANCE_3,ENDING_BALANCE_4,
    ENDING_BALANCE_5,ENDING_BALANCE_6,
    case when CRC_BUCKET_LABEL <= crc_bucket then 1 else 0 end as label
    from CSA_CARD_TEMP_ROOT_'||run_date||' r
    left join CSA_CARD_TEMP_PAID3m_'||run_date||' p on r.appl_id = p.appl_id
    left join CSA_CARD_TEMP_INSURANCE_'||run_date||' i on r.appl_id = i.appl_id
    left join CSA_CARD_TEMP_TRANCASH_'||run_date||' c on r.appl_id = c.appl_id
    left join CSA_CARD_TEMP_TRANRETAIL_'||run_date||' re on r.appl_id = re.appl_id
    left join CSA_CARD_TEMP_DPD1_'||run_date||' d1 on r.appl_id = d1.appl_id
    left join CSA_CARD_TEMP_DPD2_'||run_date||' d2 on r.appl_id = d2.appl_id
    left join CSA_CARD_TEMP_DPD3_'||run_date||' d3 on r.appl_id = d3.appl_id
    left join CSA_CARD_TEMP_DPD4_'||run_date||' d4 on r.appl_id = d4.appl_id
    left join CSA_CARD_TEMP_DPD5_'||run_date||' d5 on r.appl_id = d5.appl_id
    left join CSA_CARD_TEMP_DPD6_'||run_date||' d6 on r.appl_id = d6.appl_id
    left join CSA_CARD_TEMP_label_'||run_date||' l on r.appl_id = l.appl_id
    left join csa_card_temp_every_'||run_date||' e on r.appl_id = e.appl_id
   ';

end Csa_crc_etl_combine;
/
--endstore

/*

with a as (
select t.appl_id, d.*
from CSA_etl_b1b6_data_01062020 d
left join csa_etl_table_idno_tmp001062020 t on d.id_no = t.id_no
)
select *
from CSA_crc_etl_data_01062020 c
left join a on c.appl_id = a.appl_id
;
*/
