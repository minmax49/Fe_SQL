create or replace procedure CSA_ETL_GET_IDNO_STATUS(run_date varchar2) is
    begin
    -- split data by contract status
    Drop_table_if_exists('csa_etl_table_idno_maininfo_'||run_date);
    Drop_table_if_exists('csa_etl_table_idno_active_'||run_date);
    Drop_table_if_exists('csa_etl_table_idno_close_'||run_date);
    Drop_table_if_exists('csa_etl_table_idno_Rejected_'||run_date);

    EXECUTE IMMEDIATE ' create table csa_etl_table_idno_active_'||run_date||'
     as
     select * from csa_etl_table_idno_'||run_date||'
     where status in (''Approved'',''Active'')
     ';

     EXECUTE IMMEDIATE ' create table csa_etl_table_idno_close_'||run_date||'
     as
     select * from csa_etl_table_idno_'||run_date||'
     where status = ''Closed''
     ';

    EXECUTE IMMEDIATE ' create table csa_etl_table_idno_Rejected_'||run_date||'
    as
    select * from csa_etl_table_idno_'||run_date||'
    where status in (''Rejected'',''Cancel'')
    ';

    EXECUTE IMMEDIATE 'create table csa_etl_table_idno_maininfo_'||run_date||' as
    with a as (
    select t.id_no, t.gender, t.marital_status, t.education,  t.social_status, t.province, t.province_per,
    t.city, t.JOB_DESCRIPTION,t.birthday, t.FB_MEMBERS, t.FB_MEMBER_WORKING, t.PERSONAL_INCOME, t.FAMILY_INCOME,
    t.company_name, t.tax_code,
    ROW_NUMBER() OVER (PARTITION BY t.id_no ORDER BY nvl(t.approve_date, ''01jun1900'') DESC ) AS row_num
    from csa_etl_table_idno_'||run_date||'  t
    )
    select * from a
    where a.row_num = 1';

end CSA_ETL_GET_IDNO_STATUS;
/
--endstore


create or replace procedure CSA_ETL_GET_IDNO(appl_id_table varchar, run_date varchar2, options varchar2, build_type varchar2)
/*======================
 step 1: get ID_NO from everything
======================*/
    -- appl_id_table must have: appl_id
    -- run_date format: 'ddmmyyyy'
    -- options : id_no, appl_id , appl_id_crc
is
    begin
        declare
            reference_id varchar(50) := 'id_no';
            build_mode varchar(150) := 'create table csa_etl_table_idno_'||run_date||' as ';

        begin

            if build_type = 'insert' then
                 build_mode := 'insert into csa_etl_table_idno_'||run_date;
            else
                 Drop_table_if_exists('csa_etl_table_idno_'||run_date);
            end if;

            -- make id_no
            Drop_table_if_exists('csa_etl_table_idno_tmp0'||run_date);
            Drop_table_if_exists('csa_etl_table_idno_key'||run_date);
            Drop_table_if_exists('appl_buffer_'||run_date);

            if options = 'appl_id' then
                -- buffer table info everything
                EXECUTE IMMEDIATE 'create table csa_etl_table_idno_tmp0'||run_date||' as
                select id_no, agreement_no appl_id
                from csa_model_everything_bk e
                where exists(select 1 from '||appl_id_table||' c
                                 where e.agreement_no = c.appl_id)
                ';

            ELSIF options = 'appl_id_crc' then
                -- buffer table info everything
                EXECUTE IMMEDIATE 'create table csa_etl_table_idno_tmp0'||run_date||' as
                select id_no, e.agreement_id appl_id
                from csa_model_everything_bk e
                where exists(select 1 from '||appl_id_table||' c
                                 where e.agreement_id = c.appl_id)
                ';

            else
                -- buffer table info everything
                EXECUTE IMMEDIATE 'create table csa_etl_table_idno_tmp0'||run_date||' as
                select * from '||appl_id_table;

            end if;

            -- get infos
             EXECUTE IMMEDIATE build_mode||'
             select * from csa_model_everything_bk ee
             where exists(select 1 from csa_etl_table_idno_tmp0'||run_date||' idtable
                          where ee.id_no = idtable.id_no)'
             ;
             commit;

             -- buffer run_date for follow
             /*
             EXECUTE IMMEDIATE ' create table appl_buffer_'||run_date||' as
             select agreement_no appl_id from  csa_etl_table_idno_01062020
             where agreement_no is not null
             union
             select to_char(agreement_id) appl_id from csa_etl_table_idno_01062020
             where agreement_id is not null';
             */
             --Drop_table_if_exists('csa_etl_table_idno_tmp0'||run_date);
             -- update company name
             -- EXECUTE IMMEDIATE 'update csa_etl_table_idno_'||run_date||'
             -- set company_name = null
             -- where company_name = ''CONG TY AO CHO SP KHONG HUONG LUONG'' ';
             /* make status detail table  after insert reference data*/

        if build_type = 'insert'  then
            CSA_ETL_GET_IDNO_STATUS(run_date);
        end if;

   end;

end CSA_ETL_GET_IDNO;
/
--endstore

create or replace procedure CSA_ETL_GET_LABEL(run_date varchar2, run_mode varchar2)
as
/*======================
 step 1: get label
 input must have appl_id
======================*/
    ta_month varchar2(50) := to_char(to_date(run_date, 'ddmmyyyy'), 'mmyyyy');
    table_name varchar2(150) := 'csa_etl_table_label_'||run_date;

    begin
         Drop_table_if_exists(table_name);

         if run_mode = 'histoty' then
             EXECUTE IMMEDIATE 'create table '||table_name||' as
                with a as (
                    select ta.agreementno appl_id,
                           case when ta.Rf_Cnt = 1 then 0
                                 when ta.Rf_Cnt = 0 then 1
                                 end as  paid_case
                    from rf_ta_'||ta_month||' ta
                    where exists(select * from csa_etl_table_idno_tmp0'||run_date||' d
                    where ta.agreementno = d.appl_id)
                )
                select a.*, i.id_no
                from a
                left join csa_etl_table_idno_'||run_date||' i on a.appl_id = i.agreement_no
                order by i.id_no';
          else
             EXECUTE IMMEDIATE 'create table '||table_name||' as
                select id_no, appl_id, 0  as  paid_case
                from csa_etl_table_idno_tmp0'||run_date;
          end if;

end CSA_ETL_GET_LABEL;
/
--endstore

create or replace procedure CSA_ETL_GET_IDNO_MERGECLOSE(run_date varchar2) as
/*======================
change_status for history data
======================*/
  -- run_date format: 'ddmmyyyy'
      begin
         DECLARE
            table_name varchar2(150) := 'csa_etl_table_idno_'||run_date;
         begin
         EXECUTE IMMEDIATE  '
                begin
                Merge Into '||table_name||' eom
                Using (
                    select distinct s.appl_id
                    from csa_etl_table_dpdbom_'||run_date||'_0m s
                    where exists(select 1 from '||table_name||'  t
                    where s.appl_id = t.agreement_no)

                ) f On (eom.agreement_no=f.appl_id)
                When Matched Then Update Set eom.status = ''Active'';
                end;
             ';
          commit;
      end;
end CSA_ETL_GET_IDNO_MERGECLOSE;
/
--endstore


create or replace procedure CSA_ETL_GET_6LASTPAYMENT(run_date varchar2) as
/*======================
 step 2: get 6 Last payment
======================*/
  -- run_date format: 'ddmmyyyy'
  begin
     DECLARE
        table_name varchar2(150) := 'csa_etl_table_6payment_'||run_date;
        table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
     begin
     Drop_table_if_exists(table_name);
     EXECUTE IMMEDIATE  '
          create table '||table_name||' as
          with t as
          (select a.appl_id,a.receipt_date, a.receipt_amt,
          to_char(a.receipt_date,''mm/yyyy'') receipt_month,
          ROW_NUMBER() OVER (PARTITION BY a.Appl_id ORDER BY a.pay_date desc) AS cnt_row
          FROM sdm.sdm_col_payment_details a
          where a.pay_date between add_months(to_date('''||run_date||''',''ddmmyyyy''),-12)
                    and to_date('''||run_date||''',''ddmmyyyy'') -1
          and exists(select 1 from '||table_idno||' id where a.appl_id =  id.agreement_no)
          )
          select t.appl_id,t.receipt_date , t.receipt_amt , t.receipt_month
          from t

         ';
      commit;
      end;
end CSA_ETL_GET_6LASTPAYMENT;
/
--endstore


create or replace procedure CSA_ETL_GET_PAID3MONTH(run_date varchar2) is
/*======================
 step 3: get paid_count_3m
======================*/
  -- run_date format: 'ddmmyyyy'
  begin
    DECLARE
        table_name varchar2(50) := 'csa_etl_table_paid3month_'||run_date;
        table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
    begin
    Drop_table_if_exists(table_name);
    EXECUTE IMMEDIATE  'create table '|| table_name || '
    as
    select a.APPL_ID, count(*) as paid_count_3m, sum(a.receipt_amt) as total_paid_3m,
    avg(a.receipt_amt) avg_paid_3m
    from sdm.sdm_col_payment_details a
    where a.pay_date between add_months(to_date('''||run_date||''',''ddmmyyyy''),-3)
                and to_date('''||run_date||''',''ddmmyyyy'') -1
    and exists(select 1 from '||table_idno||' id where a.appl_id =  id.agreement_no)
    group by a.APPL_ID';

    commit;
    end;
end CSA_ETL_GET_PAID3MONTH;
/
--endstore


create or replace procedure CSA_ETL_GET_PAID6MONTH(run_date varchar2) is
/*======================
 step 4: get paid_count_6m
======================*/
  -- run_date format: 'ddmmyyyy'
  begin
    DECLARE
        table_name varchar2(50) := 'csa_etl_table_paid6month_'||run_date;
        table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
    begin
    Drop_table_if_exists(table_name);
    EXECUTE IMMEDIATE  'create table '|| table_name || '
    as
    select a.APPL_ID, count(*) as paid_count_6m, sum(a.receipt_amt) as total_paid_6m,
    avg(a.receipt_amt) avg_paid_6m
    from sdm.sdm_col_payment_details a
    where a.pay_date between add_months(to_date('''||run_date||''',''ddmmyyyy''),-6)
                and to_date('''||run_date||''',''ddmmyyyy'') -1
    and exists(select 1 from '||table_idno||' id where a.appl_id =  id.agreement_no)
    group by a.APPL_ID';
    commit;
    end;
end CSA_ETL_GET_PAID6MONTH;
/
--endstore


create or replace procedure CSA_ETL_GET_DPDBOM(run_date varchar2, month_l number) is
/*======================
 step 4: get dpd bom
======================*/
    -- run_date format: 'ddmmyyyy'
    --par_date varchar2(50) := to_char(add_months(to_date(run_date,'ddmmyyyy'), - month_l) -1, 'yyyymm');
    --table_date varchar2(50) := to_char(add_months(to_date(run_date,'ddmmyyyy'), - month_l) -1, 'yyyymmdd');
    table_date varchar2(50) := to_char(add_months(to_date(run_date,'ddmmyyyy'), -(month_l+1)), 'yyyymm');
    begin
       DECLARE
          sql_str varchar2(2000);
          table_name varchar2(50) := 'csa_etl_table_dpdbom_'||run_date||'_'||month_l||'m';
          table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
       begin
       Drop_table_if_exists(table_name);

       EXECUTE IMMEDIATE 'create table '|| table_name || '
       as
       select id.ID_NO, CONTRACT_NO appl_id, dpd, balance_amt, debt_amt, debt_penalty_amt,run_dt
       from SDM.SDM_COL_BALANCE partition(SDM_COL_BAL_FACT_'||table_date||'_2) t
       left join '||table_idno||' id on t.CONTRACT_NO = id.agreement_no
       where run_dt = to_date('||table_date||', ''yyyymmdd'')
       and contract_st = ''A''
       and exists(select 1 from '||table_idno||' id where t.CONTRACT_NO = id.agreement_no)';
       commit;
    end;
end CSA_ETL_GET_DPDBOM;
/
--endstore


create or replace procedure CSA_ETL_GET_DPDMAX(run_date varchar2, month_l number) is
/*======================
 step 5: get dpd max
======================*/
    -- run_date format: 'ddmmyyyy'

    table_date varchar2(50) := to_char(add_months(to_date(run_date,'ddmmyyyy'), - month_l), 'yyyymm');
    begin
      DECLARE
          table_name varchar2(50) := 'csa_etl_table_dpdmax_'||run_date||'_'||month_l||'m';
          table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
       begin
       Drop_table_if_exists(table_name);
       EXECUTE IMMEDIATE 'create table '|| table_name || '
        as
        with m as(
            select CONTRACT_NO appl_id, dpd
            from SDM.SDM_COL_BALANCE partition(SDM_COL_BAL_FACT_'||table_date||'_1)
            union all
            select CONTRACT_NO appl_id, dpd
            from SDM.SDM_COL_BALANCE partition(SDM_COL_BAL_FACT_'||table_date||'_2)
            )
            select m.appl_id, max(m.dpd) as dpdmax_'||month_l||'m
            from m
            where exists(select 1 from '||table_idno||' id where m.appl_id = id.agreement_no)
            group by m.appl_id
         ';
        commit;
    end;
end CSA_ETL_GET_DPDMAX;
/
--endstore

create or replace procedure CSA_ETL_GET_CONNECT(run_date varchar2, par_month varchar2) is

   follow_date varchar(50) := to_char(to_date(par_month, 'yyyy/mm'), 'yyyymm');
/*======================
 step 6: get CONNECT, group, detail
======================*/
   begin
    DECLARE
        table_name varchar2(50) := 'csa_etl_table_connectdetail_'||follow_date;
        table_namefv varchar2(50) := 'csa_etl_table_connectFV_'||follow_date;
        table_namephone varchar2(50) := 'csa_etl_table_connectPHONE_'||follow_date;
        table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
        partition_value varchar2(50);
     begin
     select partition
     into partition_value
     from csa_cs_case_details_partitions_view where month = par_month;

     Drop_table_if_exists(table_name);
     --dbms_output.put_line(' ');

     EXECUTE IMMEDIATE 'create or replace view '||table_name|| ' as
     select a.appl_id, a.contact_mode, a.remarks,a.person_contacted,
     (a.end_dt_time - a.start_dt_time) * 1440 as  contact_time, a.contact_date,
     case when nvl(a.person_contacted,''NOBODY'') <> (''NOBODY'')
     and a.response_code not in (''NAB'', ''PD_NAB'', ''PD_NC_NAB'', ''EC_NC_NAB'',
                    ''NKP'', ''PD_NC_NKP'', ''PD_NKP'', ''EC_NC_NKP'', ''PD_NKP'',
                    ''APB'', ''PD_APB'',''PA'', ''PD_PA'',
                    ''IGN5'', ''PD_IGN5'',''SUCC'',''USUC'',
                    ''HUP'', ''PD_HUP'', ''WES'', ''PD_WES'')
                    then 1 else 0 end as connect_flag,
    case when a.person_contacted is null or
        a.person_contacted in(''NOBODY'',''OTHER'',''SISTER'',''BROTHER'',''UNCLE'')
        then 0 else 1 end as contact_flag,
    case when a.response_code in (''PTP'',''PD_PTP'')
        then 1 else 0 end as ptp_flag,
    case when a.response_code in (''NAB'', ''PD_NAB'', ''PD_NC_NAB'', ''EC_NC_NAB'',
                    ''NKP'', ''PD_NC_NKP'', ''PD_NKP'', ''EC_NC_NKP'', ''PD_NKP'',
                    ''APB'', ''PD_APB'',''PA'', ''PD_PA'',
                    ''IGN5'', ''PD_IGN5'',''SUCC'',''USUC'',
                    ''HUP'', ''PD_HUP'', ''WES'', ''PD_WES'')
    then 1 else 0 end as skip_flag,
    case when a.response_code in (''F_OBT'', ''BRP'', ''TER'', ''WFP'')
        then 1 else 0 end as wac_flag,
    case when a.response_code in (''RTP'', ''IGN2'', ''IGN1'', ''GSF'', ''GSF_FID'', ''GSF_FL'',
                    ''GSF_HS'', ''GSF_NKL'', ''GSF_WA'', ''IGN3'', ''IGN4'', ''IGN5'')
    then 1 else 0 end as rtp_flag,
    case when a.response_code in (''F_WET'', ''F_WAU'', ''F_CGI'', ''LEM'',
                                ''DIE'', ''F_SOB'',''F_NAH'', ''PD_LEM'')

    then 1 else 0 end as fch_flag, a.response_code

    from DWCOLLMAIN.CS_CASE_DETAILS partition('||partition_value||') a
    where exists(select 1 from '||table_idno||' id where a.appl_id = id.agreement_no)
     ';


    -- group data
    Drop_table_if_exists(table_namefv);
    EXECUTE IMMEDIATE '
    create table '||table_namefv|| ' as
    select t.appl_id, sum(t.connect_flag) total_connect,
       sum(t.contact_flag) total_contact,
       sum(t.connect_flag)/ count(t.connect_flag) connect_rate,
       sum(t.contact_flag)/ count(t.contact_flag) contact_rate,
       sum(ptp_flag) ptp, sum(skip_flag) skip,
       sum(wac_flag) wac, sum(rtp_flag) rtp,
       sum(fch_flag) fch
    from '||table_name||' t
    where t.contact_mode = ''FV''
    group by t.appl_id';

    Drop_table_if_exists(table_namephone);
    EXECUTE IMMEDIATE '
    create table '||table_namephone|| ' as
    select t.appl_id, sum(t.connect_flag) total_connect,
       sum(t.contact_flag) total_contact,
       sum(t.connect_flag)/ count(t.connect_flag) connect_rate,
       sum(t.contact_flag)/ count(t.contact_flag) contact_rate,
       sum(ptp_flag) ptp, sum(skip_flag) skip,
       sum(wac_flag) wac, sum(rtp_flag) rtp,
       sum(fch_flag) fch
    from '||table_name||' t
    where t.contact_mode = ''PHONE''
    group by t.appl_id';

    end;
end CSA_ETL_GET_CONNECT;
/
--endstore

create or replace procedure CSA_ETL_GET_CONNECT2(run_date varchar2, par_month varchar2) is

   follow_date varchar(50) := to_char(to_date(par_month, 'yyyy/mm'), 'yyyymm');
/*======================
 step 6: get CONNECT, group, detail
======================*/
   begin
    DECLARE
        table_name varchar2(50) := 'csa_etl_table_connectdt_'||follow_date;
        table_namefv varchar2(50) := 'csa_etl_table_connectFV_'||follow_date;
        table_namephone varchar2(50) := 'csa_etl_table_connectPHONE_'||follow_date;
        table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
        partition_value varchar2(50);

     begin
     select partition
     into partition_value
     from csa_cs_case_details_partitions_view where month = par_month;

     Drop_table_if_exists(table_name);
     Drop_table_if_exists(table_namephone);
     Drop_table_if_exists(table_namefv);

     --dbms_output.put_line(' ');

     EXECUTE IMMEDIATE ' create table '||table_name||'  as
     select a.appl_id, a.contact_mode, a.remarks,a.person_contacted,
     (a.end_dt_time - a.start_dt_time) * 1440 as  contact_time, a.contact_date,
     case when nvl(a.person_contacted,''NOBODY'') <> (''NOBODY'')
     and a.response_code not in (''NAB'', ''PD_NAB'', ''PD_NC_NAB'', ''EC_NC_NAB'',
                    ''NKP'', ''PD_NC_NKP'', ''PD_NKP'', ''EC_NC_NKP'', ''PD_NKP'',
                    ''APB'', ''PD_APB'',''PA'', ''PD_PA'',
                    ''IGN5'', ''PD_IGN5'',''SUCC'',''USUC'',
                    ''HUP'', ''PD_HUP'', ''WES'', ''PD_WES'')
                    then 1 else 0 end as connect_flag,
    case when a.person_contacted is null or
        a.person_contacted in(''NOBODY'',''OTHER'',''SISTER'',''BROTHER'',''UNCLE'')
        then 0 else 1 end as contact_flag,
    case when a.response_code in (''PTP'',''PD_PTP'')
        then 1 else 0 end as ptp_flag,
    case when a.response_code in (''NAB'', ''PD_NAB'', ''PD_NC_NAB'', ''EC_NC_NAB'',
                    ''NKP'', ''PD_NC_NKP'', ''PD_NKP'', ''EC_NC_NKP'', ''PD_NKP'',
                    ''APB'', ''PD_APB'',''PA'', ''PD_PA'',
                    ''IGN5'', ''PD_IGN5'',''SUCC'',''USUC'',
                    ''HUP'', ''PD_HUP'', ''WES'', ''PD_WES'')
    then 1 else 0 end as skip_flag,
    case when a.response_code in (''F_OBT'', ''BRP'', ''TER'', ''WFP'')
        then 1 else 0 end as wac_flag,
    case when a.response_code in (''RTP'', ''IGN2'', ''IGN1'', ''GSF'', ''GSF_FID'', ''GSF_FL'',
                    ''GSF_HS'', ''GSF_NKL'', ''GSF_WA'', ''IGN3'', ''IGN4'', ''IGN5'')
    then 1 else 0 end as rtp_flag,
    case when a.response_code in (''F_WET'', ''F_WAU'', ''F_CGI'', ''LEM'', ''DIE'',
                        ''F_SOB'',''F_NAH'', ''PD_LEM'')

    then 1 else 0 end as fch_flag, a.response_code

    from DWCOLLMAIN.CS_CASE_DETAILS partition('||partition_value||') a
    --where exists(select 1 from '||table_idno||' id where a.appl_id = id.agreement_no)
     ';

    EXECUTE IMMEDIATE ' create table '||table_namephone||' as
    select t.appl_id, sum(t.connect_flag) total_connect,
       sum(t.contact_flag) total_contact,
       sum(t.connect_flag)/ count(t.connect_flag) connect_rate,
       sum(t.contact_flag)/ count(t.contact_flag) contact_rate,
       sum(ptp_flag) ptp, sum(skip_flag) skip,
       sum(wac_flag) wac, sum(rtp_flag) rtp,
       sum(fch_flag) fch
    from  '||table_name||'  t
    where t.contact_mode = ''PHONE''
    group by t.appl_id
     ';

     EXECUTE IMMEDIATE ' create table '||table_namefv||' as
    select t.appl_id, sum(t.connect_flag) total_connect,
       sum(t.contact_flag) total_contact,
       sum(t.connect_flag)/ count(t.connect_flag) connect_rate,
       sum(t.contact_flag)/ count(t.contact_flag) contact_rate,
       sum(ptp_flag) ptp, sum(skip_flag) skip,
       sum(wac_flag) wac, sum(rtp_flag) rtp,
       sum(fch_flag) fch
    from  '||table_name||'  t
    where t.contact_mode = ''FV''
    group by t.appl_id
     ';

    Drop_table_if_exists(table_name);
    end;
end CSA_ETL_GET_CONNECT2;
/
--endstore


create or replace procedure CSA_ETL_GET_FOLLOW(run_date varchar2, follow varchar2, loop_months number) is
   -- follow : 'detail' -> detail
   --        : 'FV' -->
   --        : 'PHONE' -->

   run_time date := to_date(run_date, 'ddmmyyyy');
   table_name varchar(100) := 'csa_etl_table_connect'||follow||'_'||run_date;

    begin
        declare
            month_0 varchar(50) := to_char(add_months(run_time,0), 'yyyymm');
            sqls varchar(10000) := 'create table '||table_name||' as
            select t0.*,'''||month_0||''' follow_month from csa_etl_table_CONNECT'||follow||'_'||month_0||' t0';
        begin
        Drop_table_if_exists(table_name);

        FOR counter IN 1..loop_months
        LOOP
           sqls := sqls||'
           union
           select t'||counter||'.*,'''||to_char(add_months(run_time,-counter), 'yyyymm')||''' follow_month
           from csa_etl_table_CONNECT'||follow||'_'||to_char(add_months(run_time,-counter), 'yyyymm')||' t'||counter;

        END LOOP;

       EXECUTE IMMEDIATE sqls;

    end;
end CSA_ETL_GET_FOLLOW;
/
--endstore


create or replace procedure CSA_ETL_GET_PTP(run_date varchar2, par_month varchar2) is
/*======================
 step 6: get ptp
======================*/
   begin
    DECLARE
        table_name varchar2(50) := 'csa_etl_table_ptp_'||run_date;
        table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
        partition_value varchar2(50);
     begin
     select partition
     into partition_value
     from csa_cs_case_details_partitions_view where month = par_month;

     Drop_table_if_exists(table_name);

     EXECUTE IMMEDIATE 'create table '|| table_name || '
     as
     with a as(
         select t.appl_id, t.ptp_amount, t.person_contacted,t.response_code, t.remarks,t.contact_mode,
         ROW_NUMBER() OVER (PARTITION BY  t.appl_id ORDER BY t.contact_date desc) AS cnt_row
         from DWCOLLMAIN.CS_CASE_DETAILS partition('||partition_value||') t
         where t.response_code in (''PTP'',''PD_PTP'')
         and exists(select 1 from '||table_idno||' id where t.appl_id = id.agreement_no)
     )
     select * from a
     where a.cnt_row = 1
     ';

     commit;
     end;

end CSA_ETL_GET_PTP;
/
--endstore

create or replace procedure CSA_ETL_GET_CONTACTSTATUS(run_date varchar2, par_month varchar2 ) is
/*======================
 step 10: get contact status
======================*/
   begin
    DECLARE
        table_name varchar2(50) := 'csa_etl_table_contactstatus_'||run_date;
        table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
        partition_value varchar2(50);
     begin

     select partition
     into partition_value
     from csa_cs_case_details_partitions_view where month = par_month;

     Drop_table_if_exists(table_name);
     EXECUTE IMMEDIATE 'create table '|| table_name || ' as
      with t as
          (select a.appl_id, a.contact_mode, a.contact_date, a.person_contacted, a.response_code,
          a.remarks, a.action_code_description,
          ROW_NUMBER() OVER (PARTITION BY a.Appl_id ORDER BY a.contact_date desc) AS cnt_row
          from DWCOLLMAIN.CS_CASE_DETAILS partition('||partition_value||') a
          where exists(select 1 from '||table_idno||' id where a.appl_id = id.agreement_no)
          )
      select t.appl_id, t.contact_mode , t.contact_date , t.person_contacted,
      t.response_code, t.remarks, t.action_code_description, t.cnt_row
      from t
      where t.cnt_row < 10
      ';

      commit;
      end;
end CSA_ETL_GET_CONTACTSTATUS;
/
--endstore

/*============================================================================
          --  CRE DATA --
=============================================================================*/

--endstore
create or replace procedure CSA_ETL_GET_RELATIONS(run_date varchar2) is
/*======================
 step 1: get ref relation
======================*/

    table_name varchar2(50) := 'csa_etl_table_relations_'||run_date;
    table_name2 varchar2(50) := 'csa_etl_table_relationschild_'||run_date;
    table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
    begin
         Drop_table_if_exists(table_name);
         Drop_table_if_exists(table_name||'_temp');

         EXECUTE IMMEDIATE 'create table '||table_name||'_temp  as
         select * from DW_BICC_CRE.REF_DTL_CDM dlt
         where exists(select 1 from '||table_idno||' c where dlt.nid_no = c.id_no)';

         EXECUTE IMMEDIATE 'create table '||table_name||'  as
         with aa as (
         select a.*,
         ROW_NUMBER() OVER (PARTITION BY ref_nid_no,nid_no ORDER BY  ref_phone desc) AS cnt_row,
         ROW_NUMBER() OVER (PARTITION BY substr(ref_phone,length(ref_phone)-7 ,7) ORDER BY  ref_nid_no desc) AS cnt_row2
         from '||table_name||'_temp a)
         select *
         from aa where aa.cnt_row2 = 1 or aa.cnt_row = 1
         and exists(select 1 from '||table_idno||' c where aa.nid_no = c.id_no)';


         -- FOR CHILDS
         Drop_table_if_exists(table_name2);
         EXECUTE IMMEDIATE 'create table '||table_name2||' as
         select * from DW_BICC_CRE.REF_DTL_CDM dlt
         where exists(select 1 from '||table_name||' c where dlt.nid_no = c.ref_nid_no)';

    commit;
end CSA_ETL_GET_RELATIONS;
/
--endstore

create or replace procedure CSA_ETL_GET_RELATIONS_FATHER(run_date varchar2) is
/*======================
 step 2: get ref relation father
======================*/
    table_name varchar2(50) := 'csa_etl_table_relationsfather_'||run_date;
    table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;

    begin
        Drop_table_if_exists(table_name);
        EXECUTE IMMEDIATE 'create table '||table_name||'  as
        select * from DW_BICC_CRE.REF_DTL_CDM dlt
        where exists(select 1 from '||table_idno||' c where dlt.ref_nid_no = c.id_no)';
    commit;
end CSA_ETL_GET_RELATIONS_FATHER;
/
--endstore

create or replace procedure CSA_ETL_GET_RELATIONS_IDNO(run_date varchar2) is
/*======================
 step 3: get ref relation _IDNO
======================*/
    table_name varchar2(50) := 'csa_etl_table_relationsIdno_'||run_date;
    table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;

    begin
        Drop_table_if_exists(table_name);
        EXECUTE IMMEDIATE 'create table '||table_name||'  as
        with a as (
        select REF_NID_NO ID_NO
        from csa_etl_table_RELATIONS_'||run_date||'
        union
        select NID_NO ID_NO
        from csa_etl_table_RELATIONSFATHER_'||run_date||'
        union
        select REF_NID_NO ID_NO
        from csa_etl_table_RELATIONSCHILD_'||run_date||'
        )
        select a.*
        from a

        where not exists(select 1 from '||table_idno||' c where a.id_no = c.id_no)';
    commit;
end CSA_ETL_GET_RELATIONS_IDNO;
/
--endstore

create or replace procedure CSA_ETL_GET_PHONE(run_date varchar2) is
/*======================
 step 4: get phone
======================*/
    table_name varchar2(50) := 'csa_etl_table_phone_'||run_date;
    table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;

    begin
         Drop_table_if_exists(table_name);
         EXECUTE IMMEDIATE 'create table '||table_name||' as
         with raw_phone as(
                select cc.id_no, t.*, ROW_NUMBER() OVER (PARTITION BY phone, id_no ORDER BY id_no desc) AS cnt_row
                from tmp_phone t
                left join '||table_idno||' cc on  t.appl_id = cc.agreement_no
                where exists(select 1 from '||table_idno||' c where t.appl_id = c.agreement_no)
                and phone not like ''841111%''
                and phone not like ''840000%''
        )
        select id_no, appl_id, phone, priority, phone_type
        from raw_phone
        where raw_phone.cnt_row = 1';
    commit;
end CSA_ETL_GET_PHONE;
/
--endstore

create or replace procedure CSA_ETL_GET_GEO(run_date varchar2) is
/*======================
 step 5: get lat long
======================*/
    table_name varchar2(50) := 'csa_etl_table_geo_'||run_date;
    table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;

    begin
         Drop_table_if_exists(table_name);
         EXECUTE IMMEDIATE 'create table '||table_name||' as
        select t.nid_no id_no, t.loc_lattitude, t.loc_longtitude, t.cust_addr
        from  DW_BICC_CRE.FEC_CRE_CUST_ADDR_GEOCD_PRD t
        where exists(select 1 from '||table_idno||' c where t.nid_no = c.id_no)
        and t.loc_lattitude <> -100';
    commit;
end CSA_ETL_GET_GEO;
/
--endstore


/*============================================================================
          --  CASH 24, CIC data --
=============================================================================*/

--endstore
create or replace procedure CSA_ETL_GET_CASH_24CIC(run_date varchar2) is
    table_name varchar2(50) := 'csa_etl_table_CASH_24CIC_'||run_date;
    table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;

    begin
        Drop_table_if_exists(table_name);

        EXECUTE IMMEDIATE '
        create table '||table_name||' as
        with r as (select distinct id_no from '||table_idno||'),
        ca as (
             select nationalid, substr(mobile, 2) cash24_phone,
             ROW_NUMBER() OVER (PARTITION BY nationalid,mobile ORDER BY mobile desc) AS cnt_row
             from COL_TBL_CASH24
             where exists(select 1 from '||table_idno||' s where nationalid = s.id_no)
        ),
        s37 as(
             select nationalid, phoneno s37_phone, address s37_address,
             ROW_NUMBER() OVER (PARTITION BY nationalid,phoneno ORDER BY phoneno desc) AS cnt_row
             from COL_TBL_APP_DOMINO_CIC_S37
             where exists(select 1 from '||table_idno||' s where nationalid = s.id_no)
        ),
        vmg as(
             select nid_no, phonenumber vmg_phone, HOMEADDRESS,
             ROW_NUMBER() OVER (PARTITION BY nid_no,phonenumber ORDER BY phonenumber desc) AS cnt_row
             from COL_TBL_VMG
             where exists(select 1 from '||table_idno||' s where nid_no = s.id_no)
        ),
        pcb as (
            select nationalid, perm_addr, curr_addr, mobile_phone,
            ROW_NUMBER() OVER (PARTITION BY nationalid,mobile_phone ORDER BY mobile_phone desc) AS cnt_row
            from COL_TBL_PCB_BICC
            where exists(select 1 from '||table_idno||' s where nationalid = s.id_no)
        )
        select r.id_no, ca.cash24_phone,
        vmg.vmg_phone, vmg.HOMEADDRESS vmg_address,
        s37.s37_phone, s37.s37_address,
        pcb.perm_addr pcb_perm_address, pcb.curr_addr pcb_curr_address,
        pcb.mobile_phone pcb_phone
        from  r
        left join ca  on r.id_no = ca.nationalid and ca.cnt_row = 1
        left join vmg on r.id_no = vmg.nid_no and vmg.cnt_row = 1
        left join s37 on r.id_no = s37.nationalid and s37.cnt_row = 1
        left join pcb on r.id_no = pcb.nationalid and pcb.cnt_row = 1' ;

end  CSA_ETL_GET_CASH_24CIC;
/
--endstore

--===================================================================================================
                    ------------------------------------------------
                                   --TRANFORM
                    -----------------------------------------------
--===================================================================================================

create or replace procedure CSA_ETL_tranform_IDNO(run_date varchar2) as

        table_name varchar2(150) := 'csa_etl_tranform_idno_'||run_date;
        table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;
     begin
     Drop_table_if_exists(table_name);


    EXECUTE IMMEDIATE'
    create table '||table_name||' as

    with re as(
    select id_no, count(1) rejected_num, sum(loanamount_uw) total_loanamt_uw ,
    ceil(months_between(to_Date('''||run_date||''', ''ddmmyyyy''),max(applied_date))) month_reject
    from  csa_etl_table_idno_Rejected_'||run_date||'
    group by id_no ),

    c as (select id_no, count(1) close_num, sum(loanamount) total_loanamt_closed ,
    ceil(months_between(to_Date('''||run_date||''', ''ddmmyyyy''),max(disbursal_date))) month_close
    from  csa_etl_table_idno_close_'||run_date||'
    group by id_no ),

    a as (
    select id_no, count(1) active_num, sum(loanamount) total_loanamt_active
    from  csa_etl_table_idno_active_'||run_date||'
    group by id_no )

    select a.id_no , re.rejected_num, re.total_loanamt_uw, re.month_reject,
    c.close_num, c.total_loanamt_closed, c.month_close,
    a.active_num, a.total_loanamt_active
    from a
    left join re on a.id_no =  re.id_no
    left join c on a.id_no = c.id_no';

end CSA_ETL_TRANFORM_IDNO;
/
--endstore

create or replace procedure CSA_ETL_tranform_maininfo(run_date varchar2) as
      table_name varchar2(150) := 'csa_etl_tranform_maininfo_'||run_date;
      table_name1 varchar2(150) := 'csa_etl_tranform_maininfo1_'||run_date;
      table_name2 varchar2(150) := 'csa_etl_tranform_maininfo2_'||run_date;
      table_idno varchar2(150) := 'csa_etl_table_idno_'||run_date;

      begin
          Drop_table_if_exists(table_name);
          Drop_table_if_exists(table_name1);
          Drop_table_if_exists(table_name2);

          EXECUTE IMMEDIATE 'create table  '||table_name1||'
           as
            with a as(
            select t.id_no, t.gender, t.marital_status, t.education, t.social_status ,
            t.province, t.province_per, t.curr_job_years, t.working_experience, t.personal_income, t.family_income,
            ceil(months_between(to_Date('''||run_date||''', ''ddmmyyyy''), t.birthday)/12) age,
            row_number() over (partition by id_no order by nvl(approve_date, ''01jun1800'') desc) as cnt_row
            from '||table_idno||' t)

            select * from a
            where a.cnt_row = 1';

          EXECUTE IMMEDIATE 'create table  '||table_name2||'
           as
            with a as(
            select t.id_no, t.gender, t.marital_status, t.education, t.social_status ,
            t.province, t.province_per, t.curr_job_years, t.working_experience, t.personal_income, t.family_income,
            ceil(months_between(to_Date('''||run_date||''', ''ddmmyyyy''), t.birthday)/12) age,
            row_number() over (partition by id_no order by nvl(approve_date, ''01jun1800'') desc) as cnt_row
            from '||table_idno||' t)
            select * from a
            where a.cnt_row in (2)';

          EXECUTE IMMEDIATE 'create table  '||table_name||'
          as
            select t1.id_no, nvl(t1.gender, t2.gender) gender,
            nvl(t1.marital_status,t2.marital_status) marital_status,
            nvl(t1.education, t2.education) education,
            nvl(t1.social_status, t2.social_status) social_status,
            nvl(t1.province,t2.province) province,
            nvl(t1.province_per,t2.province_per) province_per,
            nvl(t1.curr_job_years,t2.curr_job_years) curr_job_years,
            nvl(t1.working_experience,t2.working_experience) working_experience ,
            nvl(t1.personal_income,t2.personal_income) personal_income,
            nvl(t1.family_income, t2.family_income) family_income,
            nvl(t1.age , t2.age) age
            from '||table_name1||'  t1
            left join '||table_name1||'  t2 on t1.id_no = t2.id_no
            ';
end CSA_ETL_tranform_maininfo;
/
--endstore

create or replace procedure CSA_ETL_tranform_payment(run_date varchar2) as
      -- PAMENT, DPD BOM
      table_name varchar2(150) := 'csa_etl_tranform_payment_'||run_date;
      table_payment varchar2(150) := 'csa_etl_table_6payment_'||run_date;

      begin
          Drop_table_if_exists(table_name);

          EXECUTE IMMEDIATE 'create table  '||table_name||' as
          select i.id_no, pm.receipt_month, sum(pm.receipt_amt) receipt_amt,
          max(receipt_date) receipt_date, count(pm.receipt_amt) receipt_cnt
          from csa_etl_table_6payment_'||run_date||' pm
          left join csa_etl_table_idno_'||run_date||' i on pm.appl_id = i.agreement_no
          where receipt_date >= add_months(to_date('''||run_date||''', ''ddmmyyyy''), -6)
          group by i.id_no, pm.receipt_month';

end CSA_ETL_tranform_payment;
/
--endstore
create or replace procedure CSA_ETL_tranform_DPD(run_date varchar2) as
      -- PAMENT, DPD BOM
      table_name varchar2(150) := 'csa_etl_tranform_DPD_'||run_date;

      begin
          Drop_table_if_exists(table_name);

          EXECUTE IMMEDIATE 'create table  '||table_name||' as
          with a as (
          select *
          from  csa_etl_table_dpdbom_'||run_date||'_0m
          union
          select *
          from  csa_etl_table_dpdbom_'||run_date||'_1m
          union
          select *
          from  csa_etl_table_dpdbom_'||run_date||'_2m
          union
          select *
          from  csa_etl_table_dpdbom_'||run_date||'_3m

          /* union
          select *
          from  csa_etl_table_dpdbom_'||run_date||'_4m
          union
          select *
          from  csa_etl_table_dpdbom_'||run_date||'_5m
          union
          select *
          from  csa_etl_table_dpdbom_'||run_date||'_6m */
          )

          select a.id_no, a.run_dt ,min(a.dpd) min_dpd,
                 max(a.dpd) max_dpd, avg(a.dpd) avg_dpd,
                 avg(a.balance_amt) avg_balance_amt,
                 sum(a.balance_amt) total_balance_amt,
                 avg(a.debt_amt) avg_debt_amt,
                 sum(a.debt_amt) total_debt_amt

          from  a
          group by a.id_no, a.run_dt
          ';

end CSA_ETL_tranform_DPD;
/
--endstore

create or replace procedure CSA_ETL_tranform_payment_data(run_date varchar2) as
      -- PAMENT, DPD BOM
      table_name varchar2(150) := 'csa_etl_tranform_payment_data_'||run_date;

      begin
          Drop_table_if_exists(table_name);

          EXECUTE IMMEDIATE '
            create table  '||table_name||' as
            with a as (
            select d.*, nvl(p.receipt_amt, 0) receipt_amt, p.receipt_date, nvl(p.receipt_cnt,0) receipt_cnt, receipt_month
            from csa_etl_tranform_DPD_'||run_date||' d
            left join  csa_etl_tranform_payment_'||run_date||'  p
                 on d.id_no = p.id_no and to_char(d.run_dt, ''mm/yyyy'') = p.receipt_month
            where d.run_dt < to_date('''||run_date||''', ''ddmmyyyy''))

            select a.id_no, min(a.min_dpd) min_dpd_6m, max(a.max_dpd) max_dpd_6m,
                   avg(a.max_dpd) avg_dpd_6m, stddev(a.max_dpd) std_dpd_6m,
                   sum(a.receipt_amt) total_receipt_amt_6m,
                   avg(a.receipt_amt) avg_receipt_amt_6m, stddev(a.receipt_amt) std_receipt_amt_6m,
                   count(a.receipt_date) receipt_term, count(a.max_dpd) total_term,
                   sum(a.receipt_cnt) total_paid_cnt,
                   round(TO_DATE('''||run_date||''', ''ddmmyyyy'')- max(a.receipt_date) ) day_from_last_paid

            from a
            group by a.id_no';

end CSA_ETL_tranform_payment_data;
/
--endstore


create or replace procedure CSA_ETL_tranform_dpd_data(run_date varchar2) as
      --  DPD BOM status schedule
      table_name varchar2(150) := 'csa_etl_tranform_dpd_data_'||run_date;

      month0 varchar2(20) := to_char(to_date(run_date, 'ddmmyyyy'), 'ddmonyyyy');
      month1 varchar2(20) := to_char(add_months(to_date(run_date, 'ddmmyyyy'),-1), 'ddmonyyyy');
      month2 varchar2(20) := to_char(add_months(to_date(run_date, 'ddmmyyyy'),-2), 'ddmonyyyy');
      month3 varchar2(20) := to_char(add_months(to_date(run_date, 'ddmmyyyy'),-3), 'ddmonyyyy');
      month4 varchar2(20) := to_char(add_months(to_date(run_date, 'ddmmyyyy'),-4), 'ddmonyyyy');
      month5 varchar2(20) := to_char(add_months(to_date(run_date, 'ddmmyyyy'),-5), 'ddmonyyyy');
      month6 varchar2(20) := to_char(add_months(to_date(run_date, 'ddmmyyyy'),-6), 'ddmonyyyy');

      begin
          Drop_table_if_exists(table_name);

          EXECUTE IMMEDIATE '
            create table  '||table_name||' as
            with a as (
                select * from (
                 select id_no, ceil(max_dpd/30) max_dpd, run_dt
                  from csa_etl_tranform_DPD_'||run_date||'
                )
                pivot (
                   max(max_dpd) for run_dt in
                   ( '''||month0||'''  m_0, '''||month1||''' m_1, '''||month2||''' m_2,
                    '''||month3||''' m_3,'''||month4||''' m_4
                    --,'''||month5||''' m_5, '''||month6||''' m_6
                    )
                )
            )
            select a.*,
                   case when m_0 is null then ''clear''
                        when m_1 is null then ''new''
                        when m_0 > m_1 then ''rf''
                        when m_0 < m_1 then ''rb''
                        when m_0 = m_1 then ''stable'' end as status_0,

                   case when m_1 is null then ''clear''
                        when m_2 is null then ''new''
                        when m_1 > m_2 then ''rf''
                        when m_1 < m_2 then ''rb''
                        when m_1 = m_2 then ''stable'' end as status_1,

                   case when m_2 is null then ''clear''
                        when m_3 is null then ''new''
                        when m_2 > m_3 then ''rf''
                        when m_2 < m_3 then ''rb''
                        when m_2 = m_3 then ''stable'' end as status_2,

                   case when m_3 is null then ''clear''
                        when m_4 is null then ''new''
                        when m_3 > m_4 then ''rf''
                        when m_3 < m_4 then ''rb''
                        when m_3 = m_4 then ''stable'' end as status_3

            from a';

end CSA_ETL_tranform_dpd_data;
/
--endstore


create or replace procedure CSA_ETL_tranform_follow(run_date varchar2, month_l number) as
/* Follow LOAN CRC Lan lon => fix sau */
      table_name varchar2(150) := 'csa_etl_tranform_follow_'||run_date||'_'||month_l||'m';
      run_month varchar2(100) := to_char(add_months(to_date(run_date,'ddmmyyyy'), - month_l), 'yyyymm');
      begin
          Drop_table_if_exists(table_name);

          EXECUTE IMMEDIATE 'create table  '||table_name||' as
            with s1 as (
            select i.id_no, sum(c.total_connect) fv_connect_'||month_l||'m, sum(c.total_contact) fv_contact_'||month_l||'m,
                   avg(c.connect_rate) fv_connect_rate_'||month_l||'m,  avg(c.contact_rate) fv_contact_rate_'||month_l||'m,
                   sum(c.ptp) fv_ptp_'||month_l||'m, sum(c.skip) fv_skip_'||month_l||'m, sum(c.wac) fv_wac_'||month_l||'m,
                   sum(c.rtp) fv_rtp_'||month_l||'m,  sum(c.fch) fv_fch_'||month_l||'m
            from csa_etl_table_connectFV_'||run_month||' c
            left join csa_etl_table_idno_'||run_date||' i on c.appl_id = i.agreement_no
            group by i.id_no),
            s2 as (
            select i.id_no, sum(c.total_connect) phone_connect_'||month_l||'m, sum(c.total_contact) phone_contact_'||month_l||'m,
                   avg(c.connect_rate) phone_connect_rate_'||month_l||'m,  avg(c.contact_rate) phone_contact_rate_'||month_l||'m,
                   sum(c.ptp) phone_ptp_'||month_l||'m, sum(c.skip) phone_skip_'||month_l||'m, sum(c.wac) phone_wac_'||month_l||'m,
                    sum(c.rtp) phone_rtp_'||month_l||'m,  sum(c.fch) phone_fch_'||month_l||'m
            from csa_etl_table_connectphone_'||run_month||' c
            left join csa_etl_table_idno_'||run_date||' i on c.appl_id = i.agreement_no
            group by i.id_no)
            select i.id_no, s1.fv_connect_'||month_l||'m,s1.fv_contact_'||month_l||'m, s1.fv_connect_rate_'||month_l||'m,
                   s1.fv_contact_rate_'||month_l||'m,
                   s1.fv_ptp_'||month_l||'m, s1.fv_skip_'||month_l||'m, s1.fv_wac_'||month_l||'m,
                   s1.fv_rtp_'||month_l||'m, s1.fv_fch_'||month_l||'m,
                   s2.phone_connect_'||month_l||'m,s2.phone_contact_'||month_l||'m, s2.phone_connect_rate_'||month_l||'m,
                   s2.phone_contact_rate_'||month_l||'m,
                   s2.phone_ptp_'||month_l||'m, s2.phone_skip_'||month_l||'m, s2.phone_wac_'||month_l||'m,
                   s2.phone_rtp_'||month_l||'m, s2.phone_fch_'||month_l||'m
            from csa_etl_tranform_idno_'||run_date||'  i
            left join s1 on i.id_no = s1.id_no
            left join s2  on i.id_no = s2.id_no
            ';

end CSA_ETL_tranform_follow;
/
--endstore


create or replace procedure CSA_ETL_tranform_followCRC(run_date varchar2, month_l number, appl_id_table varchar2) as
      table_name varchar2(150) := 'csa_etl_tranform_followCRC_'||run_date||'_'||month_l||'m';
      run_month varchar2(100) := to_char(add_months(to_date(run_date,'ddmmyyyy'), - month_l), 'yyyymm');
      begin
          Drop_table_if_exists(table_name);

          EXECUTE IMMEDIATE 'create table  '||table_name||' as
            with s1 as (
            select c.appl_id, c.total_connect crc_fv_connect_'||month_l||'m, c.total_contact crc_fv_contact_'||month_l||'m,
                   c.connect_rate crc_fv_connect_rate_'||month_l||'m,  c.contact_rate crc_fv_contact_rate_'||month_l||'m,
                   c.ptp crc_fv_ptp_'||month_l||'m, c.skip crc_fv_skip_'||month_l||'m, c.wac crc_fv_wac_'||month_l||'m,
                   c.rtp crc_fv_rtp_'||month_l||'m,  c.fch crc_fv_fch_'||month_l||'m
            from csa_etl_table_connectFV_'||run_month||' c),
            s2 as (
            select appl_id, c.total_connect crc_phone_connect_'||month_l||'m, c.total_contact crc_phone_contact_'||month_l||'m,
                   c.connect_rate crc_phone_connect_rate_'||month_l||'m,  c.contact_rate crc_phone_contact_rate_'||month_l||'m,
                   c.ptp crc_phone_ptp_'||month_l||'m, c.skip crc_phone_skip_'||month_l||'m, c.wac crc_phone_wac_'||month_l||'m,
                   c.rtp crc_phone_rtp_'||month_l||'m,  c.fch crc_phone_fch_'||month_l||'m
            from csa_etl_table_connectphone_'||run_month||' c)
            select i.appl_id,id.id_no, s1.crc_fv_connect_'||month_l||'m, s1.crc_fv_contact_'||month_l||'m,
                   s1.crc_fv_connect_rate_'||month_l||'m, s1.crc_fv_contact_rate_'||month_l||'m,
                   s1.crc_fv_ptp_'||month_l||'m, s1.crc_fv_skip_'||month_l||'m, s1.crc_fv_wac_'||month_l||'m,
                   s1.crc_fv_rtp_'||month_l||'m, s1.crc_fv_fch_'||month_l||'m,
                   s2.crc_phone_connect_'||month_l||'m, s2.crc_phone_contact_'||month_l||'m, s2.crc_phone_connect_rate_'||month_l||'m,
                   s2.crc_phone_contact_rate_'||month_l||'m, s2.crc_phone_ptp_'||month_l||'m,
                   s2.crc_phone_skip_'||month_l||'m, s2.crc_phone_wac_'||month_l||'m,
                   s2.crc_phone_rtp_'||month_l||'m, s2.crc_phone_fch_'||month_l||'m
            from '||appl_id_table||' i
            left join csa_etl_table_idno_'||run_date||' id on i.appl_id = id.agreement_no
            left join s1 on i.applid = s1.appl_id
            left join s2  on i.applid = s2.appl_id
            ';

end CSA_ETL_tranform_followCRC;
/
--endstore


create or replace procedure CSA_ETL_LOAD(run_date varchar2) as

    table_name varchar2(150) := 'CSA_etl_b1b6_data_'||run_date;

    begin

    Drop_table_if_exists(table_name);

    EXECUTE IMMEDIATE 'create table '||table_name||' as
    select m.id_no, GENDER,MARITAL_STATUS,EDUCATION,SOCIAL_STATUS,PROVINCE,
    PROVINCE_PER,CURR_JOB_YEARS,WORKING_EXPERIENCE,PERSONAL_INCOME,FAMILY_INCOME,
    AGE,REJECTED_NUM,TOTAL_LOANAMT_UW,MONTH_REJECT,CLOSE_NUM,TOTAL_LOANAMT_CLOSED,
    MONTH_CLOSE,ACTIVE_NUM,TOTAL_LOANAMT_ACTIVE,MIN_DPD_6M,MAX_DPD_6M,AVG_DPD_6M,
    STD_DPD_6M,TOTAL_RECEIPT_AMT_6M,AVG_RECEIPT_AMT_6M,STD_RECEIPT_AMT_6M,RECEIPT_TERM,
    TOTAL_TERM,TOTAL_PAID_CNT,DAY_FROM_LAST_PAID,M_0,M_1,M_2,M_3, --M_4,M_5,M_6,
    STATUS_0,STATUS_1,STATUS_2,STATUS_3,
    FV_CONNECT_1M,FV_CONTACT_1M,FV_CONNECT_RATE_1M,
    FV_CONTACT_RATE_1M,FV_PTP_1M,FV_SKIP_1M,FV_WAC_1M,FV_RTP_1M,FV_FCH_1M,
    PHONE_CONNECT_1M,PHONE_CONTACT_1M,PHONE_CONNECT_RATE_1M,PHONE_CONTACT_RATE_1M,
    PHONE_PTP_1M,PHONE_SKIP_1M,PHONE_WAC_1M,PHONE_RTP_1M,PHONE_FCH_1M,FV_CONNECT_2M,
    FV_CONTACT_2M,FV_CONNECT_RATE_2M,FV_CONTACT_RATE_2M,FV_PTP_2M,FV_SKIP_2M,FV_WAC_2M,
    FV_RTP_2M,FV_FCH_2M,PHONE_CONNECT_2M,PHONE_CONTACT_2M,PHONE_CONNECT_RATE_2M,
    PHONE_CONTACT_RATE_2M,PHONE_PTP_2M,PHONE_SKIP_2M,PHONE_WAC_2M,PHONE_RTP_2M,
    PHONE_FCH_2M,
    -- CRC
    CRC_FV_CONNECT_1M,CRC_FV_CONTACT_1M,CRC_FV_CONNECT_RATE_1M,
    CRC_FV_CONTACT_RATE_1M,CRC_FV_PTP_1M,CRC_FV_SKIP_1M,CRC_FV_WAC_1M,CRC_FV_RTP_1M,CRC_FV_FCH_1M,
    CRC_PHONE_CONNECT_1M,CRC_PHONE_CONTACT_1M,CRC_PHONE_CONNECT_RATE_1M,CRC_PHONE_CONTACT_RATE_1M,
    CRC_PHONE_PTP_1M,CRC_PHONE_SKIP_1M,CRC_PHONE_WAC_1M,CRC_PHONE_RTP_1M,CRC_PHONE_FCH_1M,CRC_FV_CONNECT_2M,
    CRC_FV_CONTACT_2M,CRC_FV_CONNECT_RATE_2M,CRC_FV_CONTACT_RATE_2M,CRC_FV_PTP_2M,CRC_FV_SKIP_2M,CRC_FV_WAC_2M,
    CRC_FV_RTP_2M,CRC_FV_FCH_2M,CRC_PHONE_CONNECT_2M,CRC_PHONE_CONTACT_2M,CRC_PHONE_CONNECT_RATE_2M,
    CRC_PHONE_CONTACT_RATE_2M,CRC_PHONE_PTP_2M,CRC_PHONE_SKIP_2M,CRC_PHONE_WAC_2M,CRC_PHONE_RTP_2M,
    CRC_PHONE_FCH_2M,

    PAID_CASE

    from csa_etl_tranform_maininfo_'||run_date||' m
    left join csa_etl_tranform_idno_'||run_date||' c on m.id_no = c.id_no
    left join csa_etl_tranform_payment_data_'||run_date||' p on m.id_no = p.id_no
    left join csa_etl_tranform_dpd_data_'||run_date||' d on m.id_no = d.id_no
    left join csa_etl_tranform_follow_'||run_date||'_1m f1 on m.id_no = f1.id_no
    left join csa_etl_tranform_follow_'||run_date||'_2m f2 on m.id_no = f2.id_no
    left join csa_etl_tranform_followCRC_'||run_date||'_1m fc1 on m.id_no = fc1.id_no
    left join csa_etl_tranform_followCRC_'||run_date||'_2m fc2 on m.id_no = fc2.id_no
    left join (select id_no, sum(paid_case) paid_case
               from csa_etl_table_label_'||run_date||'
               group by id_no ) l on m.id_no = l.id_no
    ';

end CSA_ETL_LOAD;
/
--endstore


--- merge with CRC data
create or replace procedure CSA_ETL_MERGE_CRC(run_date varchar2) as
    table_name varchar2(150) := 'CSA_etl_crc_all_data_'||run_date;
    begin
    Drop_table_if_exists(table_name);
    EXECUTE IMMEDIATE 'create table '||table_name||' as
    with a as (
    select t.appl_id, d.*
    from CSA_etl_b1b6_data_'||run_date||' d
    left join csa_etl_table_idno_tmp0'||run_date||' t on d.id_no = t.id_no
    )
    select c.APPL_ID,a.ID_NO, c.APPLID,a.GENDER,a.MARITAL_STATUS,a.EDUCATION,a.SOCIAL_STATUS,
    a.PROVINCE,a.PROVINCE_PER,a.CURR_JOB_YEARS,a.WORKING_EXPERIENCE,
    a.PERSONAL_INCOME,a.FAMILY_INCOME,a.AGE,a.REJECTED_NUM,a.TOTAL_LOANAMT_UW,
    a.MONTH_REJECT,a.CLOSE_NUM,a.TOTAL_LOANAMT_CLOSED,a.MONTH_CLOSE,a.ACTIVE_NUM,
    a.TOTAL_LOANAMT_ACTIVE,a.MIN_DPD_6M,a.MAX_DPD_6M,a.AVG_DPD_6M,a.STD_DPD_6M,
    a.TOTAL_RECEIPT_AMT_6M,a.AVG_RECEIPT_AMT_6M,a.STD_RECEIPT_AMT_6M,a.RECEIPT_TERM,
    a.TOTAL_TERM,a.TOTAL_PAID_CNT,a.DAY_FROM_LAST_PAID,a.M_0,a.M_1,a.M_2,a.M_3,
    a.STATUS_0,a.STATUS_1,a.STATUS_2,a.STATUS_3,

    a.FV_CONNECT_1M,a.FV_CONTACT_1M,
    a.FV_CONNECT_RATE_1M,a.FV_CONTACT_RATE_1M,a.FV_PTP_1M,a.FV_SKIP_1M,a.FV_WAC_1M,
    a.FV_RTP_1M,a.FV_FCH_1M,a.PHONE_CONNECT_1M,a.PHONE_CONTACT_1M,a.PHONE_CONNECT_RATE_1M,
    a.PHONE_CONTACT_RATE_1M,a.PHONE_PTP_1M,a.PHONE_SKIP_1M,a.PHONE_WAC_1M,a.PHONE_RTP_1M,
    a.PHONE_FCH_1M,a.FV_CONNECT_2M,a.FV_CONTACT_2M,a.FV_CONNECT_RATE_2M,a.FV_CONTACT_RATE_2M,
    a.FV_PTP_2M,a.FV_SKIP_2M,a.FV_WAC_2M,a.FV_RTP_2M,a.FV_FCH_2M,a.PHONE_CONNECT_2M,
    a.PHONE_CONTACT_2M,a.PHONE_CONNECT_RATE_2M,a.PHONE_CONTACT_RATE_2M,a.PHONE_PTP_2M,
    a.PHONE_SKIP_2M,a.PHONE_WAC_2M,a.PHONE_RTP_2M,a.PHONE_FCH_2M,

    -- CRC
    CRC_FV_CONNECT_1M,CRC_FV_CONTACT_1M,CRC_FV_CONNECT_RATE_1M,
    CRC_FV_CONTACT_RATE_1M,CRC_FV_PTP_1M,CRC_FV_SKIP_1M,CRC_FV_WAC_1M,CRC_FV_RTP_1M,CRC_FV_FCH_1M,
    CRC_PHONE_CONNECT_1M,CRC_PHONE_CONTACT_1M,CRC_PHONE_CONNECT_RATE_1M,CRC_PHONE_CONTACT_RATE_1M,
    CRC_PHONE_PTP_1M,CRC_PHONE_SKIP_1M,CRC_PHONE_WAC_1M,CRC_PHONE_RTP_1M,CRC_PHONE_FCH_1M,CRC_FV_CONNECT_2M,
    CRC_FV_CONTACT_2M,CRC_FV_CONNECT_RATE_2M,CRC_FV_CONTACT_RATE_2M,CRC_FV_PTP_2M,CRC_FV_SKIP_2M,CRC_FV_WAC_2M,
    CRC_FV_RTP_2M,CRC_FV_FCH_2M,CRC_PHONE_CONNECT_2M,CRC_PHONE_CONTACT_2M,CRC_PHONE_CONNECT_RATE_2M,
    CRC_PHONE_CONTACT_RATE_2M,CRC_PHONE_PTP_2M,CRC_PHONE_SKIP_2M,CRC_PHONE_WAC_2M,CRC_PHONE_RTP_2M,
    CRC_PHONE_FCH_2M,

    c.STATEMENT_DAY,c.DUE_DATE,c.POS,c.TOTAL_AMOUNT_DUE,c.CREDIT_LIMIT,
    c.ENDING_BALANCE,c.CRC_PAID_COUNT,c.CRC_TOTAL_PAID,c.CRC_AVG_PAID,
    c.CRC_STD_PAID,c.CRC_LAST_PAID,c.CRC_LAST_PAID_AMOUNT,c.CRC_SUM_INSURANCE,
    c.CRC_COUNT_INSURANCE,c.CRC_TOTAL_CASH,c.CRC_MAX_CASH,c.CRC_COUNT_CASH,
    c.CRC_LAST_CASH,c.CRC_TOTAL_RETAIL,c.MAX_RETAIL,c.CRC_COUNT_RETAIL,
    c.CRC_LAST_RETAIL,c.CRC_DPD,c.CRC_BUCKET,
    c.CRC_DPD_1,c.CRC_DPD_2,c.CRC_DPD_3,c.CRC_DPD_4,c.CRC_DPD_5,c.CRC_DPD_6,
    c.CRC_BUCKET_1,c.CRC_BUCKET_2,c.CRC_BUCKET_3,
    c.CRC_BUCKET_4,c.CRC_BUCKET_5,c.CRC_BUCKET_6,
    c.TOTAL_AMOUNT_DUE_1,c.TOTAL_AMOUNT_DUE_2,c.TOTAL_AMOUNT_DUE_3,
    c.TOTAL_AMOUNT_DUE_4,c.TOTAL_AMOUNT_DUE_5,c.TOTAL_AMOUNT_DUE_6,
    c.ENDING_BALANCE_1,c.ENDING_BALANCE_2,c.ENDING_BALANCE_3,
    c.ENDING_BALANCE_4,c.ENDING_BALANCE_5,c.ENDING_BALANCE_6,
    c.LABEL

    from CSA_crc_etl_data_'||run_date||' c
    left join a on c.appl_id = a.appl_id'
    ;


end CSA_ETL_MERGE_CRC;
/
--endstore
