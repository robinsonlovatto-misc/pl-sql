create or replace 
package pna_cre authid current_user as
function chk_tab_exists (p_tabname in varchar2)
return pls_integer;
procedure cre_tabs (p_ts in    varchar2 := null);
procedure drop_tabs(p_log_also in varchar2 :='N');
end pna_cre;
/
create or replace 
package body pna_cre as
-- private package variables
--
function chk_tab_exists (p_tabname in varchar2)
return pls_integer is
v_tcount   pls_integer;
begin
  select count(*) into v_tcount
  from user_tables
  where table_name = upper(p_tabname);
  return v_tcount;
end chk_tab_exists;
--
procedure cre_tabs (p_ts in    varchar2 := null)
is
-- create the set of tables,
insufficient_privs    exception;
pragma exception_init (insufficient_privs, -1031);
v_ts          user_tablespaces.tablespace_name%type;
v_count       pls_integer;
v_statement   varchar2(4000);
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
    -- PLSQL_LOG is unusual, people may want to preserve it to keep logs, so we continue if it
    -- already exists
    if chk_tab_exists('PLSQL_LOG') = 0 then
      v_statement :='create table PLSQL_LOG '
                      ||' (process_name    varchar2(30) not null'
                      ||' ,start_timestamp timestamp(6) not null'
                      ||' ,log_timestamp   timestamp(6) not null'
                      ||' ,status          varchar2(1)  not null'
                      ||' ,log_level       number(1)    not null'
                      ||' ,log_text        varchar2(4000)'
                      ||' ,error_code      varchar2(10)'
                     -- ||' ,constraint dll_pk primary key (process_name,start_timestamp,log_timestamp)'
                      ||')'
                      ||' tablespace '||v_ts;
      dbms_output.put_line(v_statement);
      execute immediate v_statement;
      v_statement :='create index pllo_pnstls on plsql_log'
                      ||' (process_name,start_timestamp,log_timestamp)'
                      ||' tablespace '||v_ts;
      dbms_output.put_line(v_statement);
      execute immediate v_statement;
    else
      dbms_output.put_line ('log table already existed');
    -- for other tables, if any of them exist already, we stop.
    end if;
    v_statement :='create table fn'
                  ||' (forn_id number(5) not null'
                  ||' ,forename varchar2(30) not null'
                  ||' ,sex_ind char(1) not null'
                  ||' ,constraint  fn_pk primary key (forn_id,sex_ind) )'
                  ||' organization index';
    do_cre(v_statement);
    do_cre('create sequence forn_m_seq');
    do_cre('create sequence forn_f_seq');
--
    v_statement :='create table sn'
                  ||' (surn_id number(5) not null'
                  ||' ,surname varchar2(30) not null'
                  ||' ,constraint  sn_pk primary key (surn_id) )'
                  ||' organization index';
    do_cre(v_statement);
    do_cre('create sequence surn_seq');
--
    v_statement :='create table road_name'
                  ||' (rona_id number(5)    not null'
                  ||' ,road_name varchar2(20) not null '
                  ||' ,constraint  rona_pk primary key (rona_id) )'
                  ||' organization index';
    do_cre(v_statement);
    do_cre('create sequence rona_seq');
--
    v_statement :='create table road_type'
                  ||' (roty_id number(5)  not null'
                  ||' ,road_type varchar2(15)  not null '
                  ||' ,constraint  roty_pk primary key (roty_id) )'
                  ||' organization index';
    do_cre(v_statement);
    do_cre('create sequence roty_seq');
--
    v_statement :='create table town_name'
                  ||' (tona_id     number(5)  not null'
                  ||' ,area_name   varchar2(20)'
                  ||' ,town_name   varchar2(20)'
                  ||' ,town_county varchar2(20)'
                  ||' ,post_code   varchar2(8)'
                  ||' ,constraint  tona_pk primary key (tona_id) )'
                  ||' organization index';
    do_cre(v_statement);
    do_cre('create sequence tona_seq');
--
-- indexes not created on the main tables as they would slow down data creation.
    v_statement :='create table person'
                  ||' (pers_id             number(8)    not null'
                  ||' ,surname             varchar2(30) not null'
                  ||' ,first_forename      varchar2(30) not null'
                  ||' ,second_forename     varchar2(30)'
                  ||' ,pers_title          varchar2(10)'
                  ||' ,sex_ind             char(1)      not null'
                  ||' ,dob                 date'
                  ||' ,addr_id             number(8)'
                  ||' ,staff_ind           char(1)'
                  ||' ,last_contact_id     number(8)'
                  ||' ,pers_comment        varchar2(2000) )';
    do_cre(v_statement);
    do_cre('create sequence pers_seq cache 100 start with 12385');
--
    v_statement := 'create table address'
                  ||' (addr_id         number(8) not null'
                  ||' ,house_number    number(4)'
                  ||' ,addr_line_1     varchar2(30)'
                  ||' ,addr_line_2     varchar2(30)'
                  ||' ,addr_line_3     varchar2(30)'
                  ||' ,addr_line_4     varchar2(30)'
                  ||' ,post_code       varchar2(8) )';
    do_cre(v_statement);
    do_cre('create sequence addr_seq cache 100 start with 6392');
--
    v_statement :='create table person_name'
                  ||' (pena_id             number(8) not null'
                  ||' ,pers_id             number(8) not null'
                  ||' ,surname             varchar2(30) not null'
                  ||' ,first_forename      varchar2(30) not null'
                  ||' ,second_forename     varchar2(30)'
                  ||' ,pers_title          varchar2(10)'
                  ||' ,start_date          date'
                  ||' ,end_date            date'
                  ||' ,pena_comment        varchar2(2000) '
                  ||' ,constraint pena_pk primary key (pena_id) )';
    do_cre(v_statement);
    do_cre('create sequence pena_seq cache 100 start with 32127');
--
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
procedure drop_tabs(p_log_also in varchar2 :='N') is
procedure tab_drop(p_tab in varchar2) is
v_statement   varchar2(4000);
begin
  v_statement :='drop table '||p_tab||' purge';
  dbms_output.put_line(v_statement);
  execute immediate v_statement;
exception
  when others then
    if sqlcode=-942 then
      dbms_output.put_line('table did not exist to be drop');
    else
      raise;
   end if;
end tab_drop;
procedure seq_drop(p_seq in varchar2) is
v_statement   varchar2(4000);
begin
  v_statement :='drop sequence '||p_seq;
  dbms_output.put_line(v_statement);
  execute immediate v_statement;
exception
  when others then
    if sqlcode=-2289 then
      dbms_output.put_line('sequence did not exist to be drop');
    else
      raise;
   end if;
end seq_drop;
begin
  -- only drop log table if requsted to do so.
  if upper(p_log_also) = 'Y' then
    tab_drop('PLSQL_LOG');
  end if;
  tab_drop('SN');
  seq_drop('SURN_SEQ');
  tab_drop('FN');
  seq_drop('FORN_M_SEQ');
  seq_drop('FORN_F_SEQ');
  tab_drop('ROAD_TYPE');
  seq_drop('ROTY_SEQ');
  tab_drop('ROAD_NAME');
  seq_drop('RONA_SEQ');
  tab_drop('TOWN_NAME');
  seq_drop('TONA_SEQ');
  tab_drop('PERSON_NAME');
  seq_drop('PENA_SEQ');
  tab_drop('PERSON');
  seq_drop('PERS_SEQ');
  tab_drop('ADDRESS');
  seq_drop('ADDR_SEQ');
  dbms_output.put_line('All tables and sequences dropped');
end drop_tabs;
--
begin
  null;
end pna_cre;
/