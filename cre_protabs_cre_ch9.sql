--cre_protabs_cre.sql
create or replace 
package protabs_cre authid current_user as
procedure cre_tabs (p_ts in    varchar2 := null);
procedure drop_tabs;
end protabs_cre;
/
create or replace 
package body protabs_cre as
-- private package variables
--
procedure cre_tabs (p_ts in    varchar2 := null)
is
-- create the set of tables,
insufficient_privs    exception;
pragma exception_init (insufficient_privs, -1031);
v_ts          user_tablespaces.tablespace_name%type;
v_count       pls_integer;
v_statement   varchar2(4000);
v_m           date := trunc(sysdate,'MM');
v_m_1m        date := trunc(add_months(sysdate,-1),'MM');
v_m_2m        date := trunc(add_months(sysdate,-2),'MM');
v_m_3m        date := trunc(add_months(sysdate,-3),'MM');
v_m_1p        date := trunc(add_months(sysdate,+1),'MM');
--
procedure do_cre(p_cre in varchar2) is
begin
  dbms_output.put_line(p_cre);
  execute immediate p_cre;
exception
  when others then
    if sqlcode=-955 then
      dbms_output.put_line('Object already exists - do you need to drop first?');
    end if;
  raise;
end do_cre;
begin
  select upper(nvl(p_ts,default_tablespace)) into v_ts
  from user_users;
  select count(*) into v_count
  from user_tablespaces
  where tablespace_name = v_ts;
  if v_count = 0 then
    dbms_output.put_line ('Tablespace does not exist, tables not created');
  else
    -- we have a valid tablespace.
    -- for these tables, if any of them exist already, we stop.
    v_statement :='Create table process_master'
                  ||' (process_name        varchar2(30) not null'
                  ||' ,last_executed_timestamp timestamp(6) not null'
                  ||' ,status              varchar2(1)  not null'
                  ||' ,log_level           number(1)    not null'
                  ||' ,abandon_fl          varchar(1)   not null'
                  ||' ,process_range       number(12)'
                  ||' ,batch_size          number(5)'
                  ||' ,stage               number (2)'
                  ||' ,max_window	   number(3)'
                  ||' ,process_delay       number(6)'
                  ||' ,last_id_num1        number'
                  ||' ,last_id_num2        number'
                  ||' ,last_timestamp1     timestamp(6)'
                  ||' ,window_start_timestamp1  timestamp(6)'
                  ||' ,window_end_timestamp1    timestamp(6)'
                  ||' ,window_start_id_num1     timestamp(6)'
                  ||' ,window_end_id_num1       timestamp(6)'
                  ||' ,constraint prma_abandon_fl check (abandon_fl in (''Y'',''N''))'
                  ||' ,constraint prma_status check (status in (''C'',''I'',''R'',''A'',''E''))'
                  ||' ,constraint prma_pk primary key(process_name)'
                  ||'  )'
                  ||' tablespace '||v_ts;
    do_cre(v_statement);
--
    v_statement :='create table process_run'
                  ||' (process_name       varchar2(30) not null'
                  ||' ,start_timestamp    timestamp(6) not null'
                  ||' ,status             varchar2(1)  not null'
                  ||' ,log_level          number(1)    not null'
                  ||' ,process_range      number(12)'
                  ||' ,batch_size         number(5)'
                  ||' ,process_delay      number(6)'
                  ||' ,max_window         number(3)'
                  ||' ,start_timestamp1   timestamp(6)'
                  ||' ,start_id_num1      number'
                  ||' ,start_timestamp2   timestamp(6)'
                  ||' ,start_id_num2      number'
                  ||' ,end_timestamp1     timestamp(6)'
                  ||' ,end_id_num1        number'
                  ||' ,end_timestamp2     timestamp(6)'
                  ||' ,end_id_num2        number'
                  ||' ,completed_timestamp  timestamp(6)'
                  ||' ,records_processed  number'
                  ||' ,records_skipped    number'
                  ||' ,records_errored    number'
                  ||' ,constraint prru_status check (status in (''C'',''I'',''R'',''A'',''E''))'
                  ||' ,constraint prru_pk primary key(process_name,start_timestamp)'
                  ||' ,constraint prru_prma_fk foreign key (process_name) references process_master(process_name)'
                  ||'  )'
                  ||' tablespace '||v_ts;
    do_cre(v_statement);
--
    v_statement :='create table PROCESS_LOG '
                  ||' (process_name    varchar2(30) not null'
                  ||' ,start_timestamp timestamp(6) not null'
                  ||' ,log_timestamp   timestamp(6) not null'
                  ||' ,status          varchar2(1)  not null'
                  ||' ,log_level       number(1)    not null'
                  ||' ,log_text        varchar2(4000)'
                  ||' ,error_code      varchar2(10)'
                  ||' ,constraint prlo_prru_fk foreign key (process_name,start_timestamp)'
                  ||'             references   process_run( process_name,start_timestamp)'
                  ||')'
                  ||' tablespace '||v_ts;
    do_cre(v_statement);
    v_statement :='create index prlo_prna_stti_loti on PROCESS_LOG'
                    ||' (process_name,start_timestamp,log_timestamp)'
                    ||' tablespace '||v_ts;
    do_cre(v_statement);
--
    v_statement :='create table process_error'
                  ||' (process_name    varchar2(30) not null'
                  ||' ,start_timestamp timestamp(6)   not null'
                  ||' ,error_timestamp timestamp(6)   not null'
                  ||' ,status          varchar2(1)  not null'
                  ||' ,error_text      varchar2(4000)'
                  ||' ,error_code      varchar2(10)'
                  ||' ,constraint prer_pk primary key (process_name,start_timestamp,error_timestamp)'
                  ||' ,constraint prer_prru_fk foreign key (process_name,start_timestamp)'
                  ||'             references   process_run( process_name,start_timestamp)'
                  ||')'
                  ||' tablespace '||v_ts;
    do_cre(v_statement);
--
    v_statement :='create table customer_order'
                  ||'(id              number   not null'
                  ||',created_dt      date     not null'
                  ||',customer_id     number   not null'
                  ||',address_id      number   not null'
                  ||',status          varchar2(1)'
                  ||',completed_dt    date'
                  ||')'
                  ||' partition by range (created_dt) '
                  ||'('
                  ||' partition M'||to_char(v_m_3m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_2m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m_2m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_1m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m_1m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_1p,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' )';
    do_cre(v_statement);
    v_statement :='alter table customer_order '
                  ||'add constraint cuor_pk primary key (id,created_dt)'
                  ||'using index local';
    do_cre(v_statement);
    v_statement :='create table customer_order_line'
                  ||'(cuor_id         number   not null'
                  ||',created_dt      date     not null'
                  ||',line_no         number   not null'
                  ||',product_code    number   not null'
                  ||',unit_number     number'
                  ||',unit_cost	 number'
                  ||') '
                  ||'partition by range (created_dt) '
                  ||'('
                  ||' partition M'||to_char(v_m_3m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_2m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m_2m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_1m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m_1m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_1p,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' )';
    do_cre(v_statement);
    v_statement :='alter table customer_order_line '
                  ||'add constraint col_pk primary key (cuor_id,line_no,created_dt)'
                  ||'using index local';
    do_cre(v_statement);
    v_statement :='create table customer_order_summary'
                  ||'(id              number   not null'
                  ||',created_dt      date     not null'
                  ||',customer_id     number   not null'
                  ||',num_items       number   not null'
                  ||',tot_value       number   not null'
                  ||')'
                  ||'partition by range (created_dt)'
                  ||'('
                  ||' partition M'||to_char(v_m_3m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_2m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m_2m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_1m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m_1m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' ,partition M'||to_char(v_m,'YYYY_MM')
                  ||' values less than (to_date('''||to_char(v_m_1p,'DD-MM-YYYY')||''',''DD-MM-YYYY'')) '
                  ||' )';
    do_cre(v_statement);
    v_statement :='alter table customer_order_summary '
                  ||'add constraint cos_pk primary key (id,created_dt)'
                  ||'using index local';
    do_cre(v_statement);
  end if;
exception
  when insufficient_privs then
    dbms_output.put_line('you lack the direct priveleges to carry out this task - remember, roles are ignored by PL/SQL');
    raise;
  when others then
    dbms_output.put_line(dbms_utility.format_error_stack);
    dbms_output.put_line(dbms_utility.format_error_backtrace);
end cre_tabs;
--
--
procedure drop_tabs is
procedure tab_drop(p_tab in varchar2) is
v_statement   varchar2(4000);
begin
  v_statement :='drop table '||p_tab||' purge';
  dbms_output.put_line(v_statement);
  execute immediate v_statement;
exception
  when others then
    if sqlcode=-942 then
      dbms_output.put_line('table did not exist to be dropped');
    else
      raise;
   end if;
end tab_drop;
begin
  tab_drop('PROCESS_ERROR');
  tab_drop('PROCESS_LOG');
  tab_drop('PROCESS_RUN');
  tab_drop('PROCESS_MASTER');
--
  tab_drop('CUSTOMER_ORDER_SUMMARY');
  tab_drop('CUSTOMER_ORDER_LINE');
  tab_drop('CUSTOMER_ORDER');
  dbms_output.put_line('All tables dropped');
end drop_tabs;
--
begin
  null;
end protabs_cre;
/