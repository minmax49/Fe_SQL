create or replace procedure Csa_loan_etl_root(run_date varchar2, build_type varchar2) is
    /*
       -- ROOT step
       GEt get appl_id, pos_bom , dpd,  bucket, ticket, label 
       -- build_type ('history', 'today')
    */
  BEGIN 
      DECLARE
      -- columns name 
      table_root varchar2(100) := 'coll_VPmain.cs_case_info@LOSDR';
      pos_col varchar2(100) := 'principle_outstanding';
      appl_id varchar2(100) := 'appl_id';
      -- config sql
      sql_close_flag varchar2(100) := 'and account_closed_flg <> ''N'' ';
      sql_label varchar2(300) := '0 as label ';
      -- 
      table_name varchar2(150) := 'csa_loan_temp_root_'||run_date;
      
      BEGIN
        -- get appl_id, pos_bom , dpd,  bucket, ticket
        if build_type = 'history' then 
            table_root := 'RF_TA_'||substr(run_date, 3, 6);
            pos_col  := 'out_principals';
            sql_close_flag   := '  ';
            appl_id := 'agreementno';
            sql_label  := 'case when Rf_Cnt = 1 then 0
                                       when Rf_Cnt = 0 then 1
                                       end as paid_case';                          
        end if;
        
        Drop_table_if_exists(table_name);
        -- for run_time
        EXECUTE IMMEDIATE ' create table '||table_name|| '
        as
        select to_date('''||run_date||''', ''ddmmyyyy'') as run_date, '||appl_id||' appl_id,
         '||pos_col||' as pos_bom, a.dpd, ceil(a.dpd/30) as bucket,
         case 
           when a.product in (''FC_PL'',''FC_PL_X'',''PL'') and  '||pos_col||' > 35000000 then ''HIGH''
           when a.product in (''FC_PL'',''FC_PL_X'',''PL'') and  '||pos_col||' between 5000000 and 35000000 then ''MEDIUM''
           when a.product in (''FC_PL'',''FC_PL_X'',''PL'') and  '||pos_col||' < 5000000 then ''LOW''
           when a.product in (''CDL'',''FC_CDL'',''FC_CDL_G'') and  '||pos_col||' > 10000000 then ''HIGH''
           when a.product in (''CDL'',''FC_CDL'',''FC_CDL_G'') and  '||pos_col||' between 3000000 and 10000000 then ''MEDIUM''
           when a.product in (''CDL'',''FC_CDL'',''FC_CDL_G'') and  '||pos_col||' < 3000000 then ''LOW''
           when a.product in (''TW'',''FC_TW'') and  '||pos_col||' >15000000 then ''HIGH''
           when a.product in (''TW'',''FC_TW'') and  '||pos_col||' between 3000000 and 15000000 then ''MEDIUM''
           when a.product in (''TW'',''FC_TW'') and  '||pos_col||' < 3000000 then ''LOW''
           else ''CHECK'' 
         end as TICKET,    
         '||sql_label||'     
                       
        from '||table_root||' a
        where a.dpd  between 1 and 180
        '||sql_close_flag||'
        and rownum < 100
       ';
       
        -- dpd defer case
        EXECUTE IMMEDIATE ' Merge Into '||table_name||' eom
        Using (
          select t.agreementno appl_id, t.dpd, ceil(t.dpd/30) bucket
          from csa_daily_defer_dpd t 
          where t.run_date = to_date('''||run_date||''', ''ddmmyyyy'') 
        ) f On (eom.appl_id=f.appl_id)
        When Matched Then Update Set eom.dpd = f.dpd, eom.bucket = f.bucket
        ';
        commit;
   
   END;
end Csa_loan_etl_root;
/
--endstore


create or replace procedure Csa_loan_etl_everything(run_date varchar2) is
BEGIN
  DECLARE
    table_name varchar2(150) := 'csa_loan_temp_every_' || run_date;
    table_root varchar2(150) := 'csa_loan_temp_root_'||run_date;
  BEGIN
    Drop_table_if_exists(table_name);

    EXECUTE IMMEDIATE 'create table '||table_name||'
    as
    select a.appl_id, c.cus_id, app_id, eff_rate, loanamount,
    ceil(MONTHS_BETWEEN(to_date('''||run_date||''', ''ddmmyyyy''), c.disbursal_date)) MOB
    from '||table_root||' a
    left join sdm.sdm_col_everything c on a.appl_id = c.agreement_no
    ';

  end;
end Csa_loan_etl_everything;
/
--endstore


create or replace procedure Csa_loan_etl_payment6m(run_date varchar2) is
  /*
   -- step : payment 6month (mabe 7)
   GEt get appl_id, paid_count_6m , total_paid_6m,  avg_paid_6m, std_paid_6m
   -- build_type ('history', 'today')
  */        
  begin
     DECLARE
        table_name varchar2(50) := 'csa_loan_temp_payment6m_'||run_date;
        table_root varchar2(50) := 'csa_loan_temp_root_'||run_date;

     begin
         Drop_table_if_exists(table_name||'_temp');
         Drop_table_if_exists(table_name||'_1');
         Drop_table_if_exists(table_name||'_2');
         Drop_table_if_exists(table_name);
         
         -- buffer payment table 
         EXECUTE IMMEDIATE 'create table '||table_name||'_temp 
         as
         select a.APPL_ID, a.pay_date, receipt_date, receipt_amt
         from sdm.sdm_col_payment_details a
         where a.pay_date between add_months(to_date('''||run_date||''',''ddmmyyyy''),-7)
                    and to_date('''||run_date||''',''ddmmyyyy'') -1
         and exists(select 1 from '||table_root||' r where a.appl_id = r.appl_id) 
         '; 
         
         -- series payments
         EXECUTE IMMEDIATE 'create table '||table_name||'_1
         as
         select a.APPL_ID, count(*) as paid_count_6m, 
         sum(a.receipt_amt) as total_paid_6m,
         avg(a.receipt_amt) avg_paid_6m, stddev(a.receipt_amt) std_paid_6m
         from '||table_name||'_temp a
         group by a.APPL_ID'; 
         
         -- last paid
         EXECUTE IMMEDIATE 'create table '|| table_name||'_2  as
          with t as
          (select a.appl_id,a.receipt_date, a.receipt_amt,
          ROW_NUMBER() OVER (PARTITION BY a.Appl_id ORDER BY a.pay_date desc) AS cnt_row
          FROM '||table_name||'_temp a)
          select t.appl_id,t.receipt_date,
          to_date('''||run_date||''',''ddmmyyyy'') - t.receipt_date day_from_last_paid, 
          t.receipt_amt
          from t
          where t.cnt_row = 1'; 
          
         -- merge table 
         EXECUTE IMMEDIATE 'create table '|| table_name||'  as
         select t1.*, t2.day_from_last_paid, t2.receipt_amt
         from  '||table_name||'_1 t1 
         left join  '||table_name||'_2 t2 on t1.appl_id = t2.appl_id
         ';  
                
         Drop_table_if_exists(table_name||'_temp');
         Drop_table_if_exists(table_name||'_1');
         Drop_table_if_exists(table_name||'_2');
         
     end;
end Csa_loan_etl_payment6m;
/
--endstore

create or replace procedure Csa_loan_etl_dpd(run_date varchar2, month_l number) is
    /*
   -- step : get history dpd 
   -- month_l : month want to get dpd
   */     
    table_date varchar2(50) := to_char(add_months(to_date(run_date,'ddmmyyyy'), -(month_l+1)), 'yyyymm');
    --  -month_l-1 : partition by balance_dt but dpd by run_dt , run_dt = balance_dt + 1
    begin
      DECLARE
          table_name varchar2(50) := 'csa_loan_temp_dpd'||month_l||'_'||run_date;
          table_root varchar2(50) := 'csa_loan_temp_root_'||run_date;
       begin
       Drop_table_if_exists(table_name);
       EXECUTE IMMEDIATE 'create table '|| table_name || '
        as
        select CONTRACT_NO appl_id, dpd as dpd_'||month_l||'m
        from SDM.SDM_COL_BALANCE partition(SDM_COL_BAL_FACT_'||table_date||'_2) t 
        where run_dt = to_date('''||run_date||''', ''ddmmyyyy'') 
        and exists(select 1 from '||table_root||' r where t.CONTRACT_NO = r.appl_id) 
        ';
        
        -- dpd defer case
        EXECUTE IMMEDIATE ' Merge Into '||table_name||' eom
        Using (
          select t.agreementno appl_id, t.dpd
          from csa_daily_defer_dpd t 
          where t.run_date = to_date('''||run_date||''', ''ddmmyyyy'') 
        ) f On (eom.appl_id=f.appl_id)
        When Matched Then Update Set eom.dpd_'||month_l||'m = f.dpd
        ';
        commit;
         
    end;
end Csa_loan_etl_dpd;
/
--endstore

create or replace procedure Csa_loan_etl_TELCOSCORE(run_date varchar2) is
   /*
   -- step : get history dpd 
   -- 
   */     
    begin
      DECLARE
          table_name varchar2(50) := 'csa_loan_temp_TELCOSCORE_'||run_date;
          table_root varchar2(50) := 'csa_loan_temp_root_'||run_date;
       begin
       Drop_table_if_exists(table_name);
       EXECUTE IMMEDIATE 'create table '||table_name || '
        as
        select agreement_no appl_id, s.leadsource_last, s.telco_score_trend, s.telco_score_avg, 
        s.telco_score_last, s.score_request_cnt,
        to_date('''||run_date||''',''ddmmyyyy'') - s.requestdate_last day_from_last_request
        from risk_nhutltm.POL_TBL_RAW_TELCOSCORE s
        where exists(select * from '||table_root||' r where s.agreement_no = r.appl_id) 
        ';
    end;
end Csa_loan_etl_TELCOSCORE;
/
--endstore


