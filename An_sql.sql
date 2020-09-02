create or replace procedure dbadw.call_common_credit_card_jobs
is
  v_errnbr number;
  v_errmsg varchar2(200);
  v_exception exception;

  v_job_name varchar2(128);
  v_rundate  varchar2(50);
  v_ircptlist varchar2(500);
  v_num_jobs_completed number:=0;
  v_job_name_prefix varchar2(30);
begin
  v_ircptlist := 'an.nguyen.12@fecredit.com.vn,tuyen.nguyen.43@fecredit.com.vn,mai.huynh.1@fecredit.com.vn';
  v_job_name_prefix:='J_CRC';
  v_rundate := to_char(sysdate, 'MONDD');
  /* Tach rieng job chay luc 7h
  --01. Call RUN_COL_SP_CE_DAILY_DTL_CC Proc
  v_job_name:= v_job_name_prefix || '_DAILY_DTL_CC'||v_rundate;

  begin
      dbms_scheduler.create_job(job_name            => v_job_name,
                                job_type            => 'STORED_PROCEDURE',
                                job_action          => 'COMMON.RUN_COL_SP_CE_DAILY_DTL_CC',
                                number_of_arguments => 1,
                                comments            => 'Call proc COMMON.RUN_COL_SP_CE_DAILY_DTL_CC',
                                enabled             => false,
                                auto_drop           => true);
      dbms_scheduler.set_job_argument_value(job_name          => v_job_name,
                                            argument_position => 1,
                                            argument_value    => trunc(sysdate));

      dbms_scheduler.set_attribute(name      => v_job_name,
                                   attribute => 'Instance_Stickiness',
                                   value     => false);
      -- dbms_scheduler.run_job(job_name => v_job_name);
      dbms_scheduler.enable(name => v_job_name);
      begin
        insert into common_credit_card_job_monitor(job_name,proc_name,rundate,status)
        values(v_job_name, 'COMMON.RUN_COL_SP_CE_DAILY_DTL_CC', sysdate, 'RUNNING');
        commit;
        pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [RUN_COL_SP_CE_DAILY_DTL_CC]' ,iBody => 'JOB '|| v_job_name || ' STARTED');
      exception
        when others then
          rollback;
          pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [RUN_COL_SP_CE_DAILY_DTL_CC]' ,iBody => 'JOB '|| v_job_name || ' FAILED');
      end;
   end;
  */
   --02. Call CRC_MAI_REPORT_DELI
   v_job_name:= v_job_name_prefix ||'_RPT_DELI'||v_rundate;
   begin
      dbms_scheduler.create_job(job_name            => v_job_name,
                                job_type            => 'STORED_PROCEDURE',
                                job_action          => 'COMMON.CRC_MAI_REPORT_DELI',
                                number_of_arguments => 0,
                                comments            => 'Call proc COMMON.CRC_MAI_REPORT_DELI',
                                enabled             => false,
                                auto_drop           => true);

      dbms_scheduler.set_attribute(name      => v_job_name,
                                   attribute => 'Instance_Stickiness',
                                   value     => false);
      -- dbms_scheduler.run_job(job_name => v_job_name);
      dbms_scheduler.enable(name => v_job_name);
      begin
        insert into common_credit_card_job_monitor(job_name,proc_name,rundate,status)
        values(v_job_name, 'COMMON.CRC_MAI_REPORT_DELI', sysdate, 'RUNNING');
        commit;
        pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [CRC_MAI_REPORT_DELI]' ,iBody => 'JOB '|| v_job_name || ' STARTED');
      exception
        when others then
          rollback;
          pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [CRC_MAI_REPORT_DELI]' ,iBody => 'JOB '|| v_job_name || ' FAILED');
      end;
   end;

   --

   --03. Call CRC_MAI_REPORT_PREDUE
   v_job_name:= v_job_name_prefix||'_RPT_PREDUE'||v_rundate;
   begin
      dbms_scheduler.create_job(job_name            => v_job_name,
                                job_type            => 'STORED_PROCEDURE',
                                job_action          => 'COMMON.CRC_MAI_REPORT_PREDUE',
                                number_of_arguments => 0,
                                comments            => 'Call proc COMMON.CRC_MAI_REPORT_PREDUE',
                                enabled             => false,
                                auto_drop           => true);

      dbms_scheduler.set_attribute(name      => v_job_name,
                                   attribute => 'Instance_Stickiness',
                                   value     => false);
      -- dbms_scheduler.run_job(job_name => v_job_name);
      dbms_scheduler.enable(name => v_job_name);
      begin
        insert into common_credit_card_job_monitor(job_name,proc_name,rundate,status)
        values(v_job_name, 'COMMON.CRC_MAI_REPORT_PREDUE', sysdate, 'RUNNING');
        commit;
        pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [CRC_MAI_REPORT_PREDUE]' ,iBody => 'JOB '|| v_job_name || ' STARTED');
      exception
        when others then
          rollback;
          pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [CRC_MAI_REPORT_PREDUE]' ,iBody => 'JOB '|| v_job_name || ' FAILED');
      end;
   end;

    --04. Call COL_SP_CRC_AUTO_PREDUE
   v_job_name:= v_job_name_prefix||'_AUTO_PREDUE'||v_rundate;
   begin
      dbms_scheduler.create_job(job_name            => v_job_name,
                                job_type            => 'STORED_PROCEDURE',
                                job_action          => 'COMMON.COL_SP_CRC_AUTO_PREDUE',
                                number_of_arguments => 0,
                                comments            => 'Call proc COL_SP_CRC_AUTO_PREDUE',
                                enabled             => false,
                                auto_drop           => true);

      dbms_scheduler.set_attribute(name      => v_job_name,
                                   attribute => 'Instance_Stickiness',
                                   value     => false);
      -- dbms_scheduler.run_job(job_name => v_job_name);
      dbms_scheduler.enable(name => v_job_name);
      begin
        insert into common_credit_card_job_monitor(job_name,proc_name,rundate,status)
        values(v_job_name, 'COL_SP_CRC_AUTO_PREDUE', sysdate, 'RUNNING');
        commit;
        pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [COL_SP_CRC_AUTO_PREDUE]' ,iBody => 'JOB '|| v_job_name || ' STARTED');
      exception
        when others then
          rollback;
          pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [COL_SP_CRC_AUTO_PREDUE]' ,iBody => 'JOB '|| v_job_name || ' FAILED');
      end;
   end;

   --05. Call COL_SP_CRC_AUTO_B1NEW
   v_job_name:= v_job_name_prefix||'_AUTO_B1NEW'||v_rundate;
   begin
      dbms_scheduler.create_job(job_name            => v_job_name,
                                job_type            => 'STORED_PROCEDURE',
                                job_action          => 'COMMON.COL_SP_CRC_AUTO_B1NEW',
                                number_of_arguments => 0,
                                comments            => 'Call proc COL_SP_CRC_AUTO_B1NEW',
                                enabled             => false,
                                auto_drop           => true);

      dbms_scheduler.set_attribute(name      => v_job_name,
                                   attribute => 'Instance_Stickiness',
                                   value     => false);
      -- dbms_scheduler.run_job(job_name => v_job_name);
      dbms_scheduler.enable(name => v_job_name);
      begin
        insert into common_credit_card_job_monitor(job_name,proc_name,rundate,status)
        values(v_job_name, 'COL_SP_CRC_AUTO_B1NEW', sysdate, 'RUNNING');
        commit;
        pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [COL_SP_CRC_AUTO_B1NEW]' ,iBody => 'JOB '|| v_job_name || ' STARTED');
      exception
        when others then
          rollback;
          pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [COL_SP_CRC_AUTO_B1NEW]' ,iBody => 'JOB '|| v_job_name || ' FAILED');
      end;
   end;

    --06. Call COL_SP_CRC_AUTO_BOM
   v_job_name:= v_job_name_prefix||'_AUTO_BOM'||v_rundate;
   begin
      dbms_scheduler.create_job(job_name            => v_job_name,
                                job_type            => 'STORED_PROCEDURE',
                                job_action          => 'COMMON.COL_SP_CRC_AUTO_BOM',
                                number_of_arguments => 0,
                                comments            => 'Call proc COMMON.COL_SP_CRC_AUTO_BOM',
                                enabled             => false,
                                auto_drop           => true);

      dbms_scheduler.set_attribute(name      => v_job_name,
                                   attribute => 'Instance_Stickiness',
                                   value     => false);
      -- dbms_scheduler.run_job(job_name => v_job_name);
      dbms_scheduler.enable(name => v_job_name);
      begin
        insert into common_credit_card_job_monitor(job_name,proc_name,rundate,status)
        values(v_job_name, 'COMMON.COL_SP_CRC_AUTO_BOM', sysdate, 'RUNNING');
        commit;
        pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [COL_SP_CRC_AUTO_BOM]' ,iBody => 'JOB '|| v_job_name || ' STARTED');
      exception
        when others then
          rollback;
          pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [COL_SP_CRC_AUTO_BOM]' ,iBody => 'JOB '|| v_job_name || ' FAILED');
      end;
   end;

    --07. Call SP_CRC_GLX_PRE_NEW
   v_job_name:= v_job_name_prefix||'_GLX_PRE_NEW'||v_rundate;
   begin
      dbms_scheduler.create_job(job_name            => v_job_name,
                                job_type            => 'STORED_PROCEDURE',
                                job_action          => 'COMMON.SP_CRC_GLX_PRE_NEW',
                                number_of_arguments => 0,
                                comments            => 'Call proc COMMON.SP_CRC_GLX_PRE_NEW',
                                enabled             => false,
                                auto_drop           => true);

      dbms_scheduler.set_attribute(name      => v_job_name,
                                   attribute => 'Instance_Stickiness',
                                   value     => false);
      -- dbms_scheduler.run_job(job_name => v_job_name);
      dbms_scheduler.enable(name => v_job_name);
      begin
        insert into common_credit_card_job_monitor(job_name,proc_name,rundate,status)
        values(v_job_name, 'COMMON.SP_CRC_GLX_PRE_NEW', sysdate, 'RUNNING');
        commit;
        pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [SP_CRC_GLX_PRE_NEW]' ,iBody => 'JOB '|| v_job_name || ' STARTED');
      exception
        when others then
          rollback;
          pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [SP_CRC_GLX_PRE_NEW]' ,iBody => 'JOB '|| v_job_name || ' FAILED');
      end;
   end;

     --08. Call CRC_DELINQUNET_MATRIX
   v_job_name:= v_job_name_prefix||'_DELINQUNET_MATRIX'||v_rundate;
   begin
      dbms_scheduler.create_job(job_name            => v_job_name,
                                job_type            => 'STORED_PROCEDURE',
                                job_action          => 'COMMON.COL_SP_CRC_DELINQUNET_MATRIX',
                                number_of_arguments => 0,
                                comments            => 'Call proc COL_SP_CRC_DELINQUNET_MATRIX',
                                enabled             => false,
                                auto_drop           => true);

      dbms_scheduler.set_attribute(name      => v_job_name,
                                   attribute => 'Instance_Stickiness',
                                   value     => false);
      -- dbms_scheduler.run_job(job_name => v_job_name);
      dbms_scheduler.enable(name => v_job_name);
      begin
        insert into common_credit_card_job_monitor(job_name,proc_name,rundate,status)
        values(v_job_name, 'COMMON.COL_SP_CRC_DELINQUNET_MATRIX', sysdate, 'RUNNING');
        commit;
        pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [DELINQUNET_MATRIX]' ,iBody => 'JOB '|| v_job_name || ' STARTED');
      exception
        when others then
          rollback;
          pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card [DELINQUNET_MATRIX]' ,iBody => 'JOB '|| v_job_name || ' FAILED');
      end;
   end;

   --
   v_num_jobs_completed := 0;

  while (v_num_jobs_completed < 7) loop
     SELECT count(distinct(job_name))
    INTO v_num_jobs_completed
    from user_scheduler_job_run_details
    where job_name like v_job_name_prefix || '%'||v_rundate
    and to_char(log_date, 'DD-MM-YYYY') = to_char(sysdate, 'DD-MM-YYYY');

    for i in ( select r.JOB_NAME,r.STATUS, r.RUN_DURATION, substr( r.ADDITIONAL_INFO , 200) ADDITIONAL_INFO
                from user_scheduler_job_run_details r
                where r.JOB_NAME like v_job_name_prefix || '%'||v_rundate
                and to_char(log_date, 'DD-MM-YYYY') = to_char(sysdate, 'DD-MM-YYYY')
                and exists (select 1 from dbadw.common_credit_card_job_monitor c
                                where c.job_name = r.job_name
                                and trunc(c.rundate) = trunc(sysdate)
                                and c.STATUS = 'RUNNING')
                )
     loop

     update dbadw.common_credit_card_job_monitor c
       set c.status=i.status
       where c.job_name = i.job_name
       and trunc(c.rundate) = trunc(sysdate);
     end loop;

    sys.dbms_lock.sleep(500); -- 2 mins
  end loop;
  pr_send_email(ircptlist => v_ircptlist ,iSubject =>'[Important] Job Credit Card is Done' ,iBody => 'JOB '|| v_job_name || ' DONE');

   --
end;
