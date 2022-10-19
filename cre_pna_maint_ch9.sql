create or replace 
package pna_maint as
function num_inc(p_text in varchar2
                ,p_incr in pls_integer :=1)
return varchar2;
-- I include these rather than use my str_util package to simplify life for people using this code
-- to reproduce my tests.
function piece (p_string    in varchar2
               ,p_start     in pls_integer
               ,p_end       in pls_integer   := null
               ,p_delimiter in varchar2 := ',')
return varchar2;
function piece (p_string    in varchar2
               ,p_start     in pls_integer
               ,p_delimiter in varchar2 := ',')
return varchar2;
procedure get_prma (p_process_name in process_master.process_name%type
                   ,p_upd         in boolean  :=true);
procedure pl (v_text in varchar2);
procedure test1;
procedure test_auto;
procedure pop_source(p_log_level in pls_integer := 5);
procedure pop_addr_rbyr(p_rows   in pls_integer :=1000000);
procedure pop_addr_batch(p_rows      in pls_integer :=1000000
                        ,p_log_level in pls_integer :=5);
procedure pop_addr_h;
procedure pop_pers(p_families    in pls_integer :=100000
                  ,p_log_level   in pls_integer :=5);
procedure pop_cuor_col(p_days    in pls_integer :=10
                      ,p_start   in date :=trunc(sysdate-10)
                      ,p_log_level in pls_integer :=3
                      ,p_recs_per_day in pls_integer :=1000);
procedure summarize_orders;
procedure trunc_tabs(p_log_also  in varchar2 :='N');
end pna_maint;
/
create or replace 
package body pna_maint as
-- private package variables
pv_err_msg             varchar2(2000);
pv_err_stack           varchar2(2000);
pv_call_stack          varchar2(2000);
pv_process_name        process_master.process_name%type;
pv_executed_timestamp  process_run.start_timestamp%type;
pv_log_level           process_master.log_level%type;
pv_init_module         varchar2(48);
pv_init_action         varchar2(32);
pv_prma_rec            process_master%rowtype; -- you could specify a variable per col if
pv_prru_rec            process_run%rowtype;    -- you are more comfortable with that...
function num_inc(p_text in varchar2
                ,p_incr in pls_integer :=1)
return varchar2 is
v_return varchar2(4000);
v_translate     varchar2(4000);
v_text          varchar2(4000);
v_num_start     pls_integer;
v_num_end       pls_integer;
v_length        pls_integer;
v_num_text      varchar2(4000);
v_number        pls_integer;
v_incr          pls_integer;
begin
  v_text := substr(p_text,1,4000);
  v_incr := p_incr;
  v_translate := translate(v_text,'1234567890'
                                 ,'9999999999');
  if instr(v_translate,'9') = 0 then
    v_return := v_text;
  else
    v_num_start := instr(v_translate,'9');
    v_num_end   := instr(v_translate,'9',-1);
    v_length    := (v_num_end-v_num_start)+1;
    v_num_text  := substr(v_text,v_num_start,v_length);
    if rpad('9',v_length,'9') != substr(v_translate,v_num_start,v_length) then
      -- the extracted number holds non-numerics!
      v_return := v_text;
    else
      v_return := substr(v_text,1,v_num_start-1)
                  ||to_char(v_num_text+v_incr)
                  ||substr(v_text,v_num_end+1);
    end if;
  end if;
  return v_return;
end;
--
function piece (p_string in varchar2
               ,p_start     in pls_integer
               ,p_end       in pls_integer   := null
               ,p_delimiter in varchar2 := ',')
return varchar2
-- Accept string; first piece wanted; optional to end piece; optional delimiter
-- ',' as default.
-- If parameters invalid, return null
-- If first piece exists but string ends before end piece found, return
-- whole of string from first piece.
-- If a range of pieces requested, delimiters within the returned string remain.
is
v_string    varchar2(2000) := p_string;
v_start     pls_integer    := floor(abs(p_start));
v_end       number         := floor(abs(p_end));
v_delimiter varchar2(20)   := p_delimiter;
v_return    varchar2(2000) := null;
v_dlen      pls_integer    :=length(v_delimiter);
v_from      pls_integer;
v_to        pls_integer;
begin
if v_string is null
or v_start is null
or v_delimiter is null then -- missing parameters
  null;
elsif v_start > 1
and   instr(v_string,v_delimiter,1,greatest(v_start-1,1)) =0 then -- no nth part
  null;
elsif v_end <v_start then
  null;
else -- all parameters make sense. Maybe this is all overkill, but
     -- might as well do correctly.
  if v_start <2 then      -- ie want first part
    v_from :=1;           -- ensure v_from is 1
    v_start :=1;          -- Start at begining of string
  else
    v_from := instr(v_string,v_delimiter,1,v_start-1)+v_dlen;
  end if;
  if v_end is null then
    v_end := v_start;
  end if;
  v_to := instr(v_string,v_delimiter,1,v_end) -1;
  if v_to = -1 then
    v_to := length(v_string);
  end if;
  v_return := substr(v_string
                    ,v_from
                    ,(v_to-v_from)+1);
end if;
return v_return;
end piece;
-- overloaded function to allow simple "this piece only" usage
function piece (p_string in varchar2
               ,p_start     in pls_integer
               ,p_delimiter in varchar2 := ',')
return varchar2
is
v_temp varchar2(2000);
begin
v_temp := piece(p_string,p_start,null,p_delimiter);
return v_temp;
end piece;
--
procedure pl (v_text in varchar2) is
begin
  dbms_output.put_line(to_char(systimestamp,'HH24:MI:SS.FF3 :')||v_text);
end pl;
--
procedure write_log(v_log_text   in varchar2
                   ,v_status     in varchar2 :=''
                   ,v_error_code in  varchar2 :=''
                   )
-- needs to be an autonomous transaction otherwise it would not be observable during execution and would potentially be
-- lost in a controlled rollback.
is
pragma autonomous_transaction;
v_timestamp      timestamp :=systimestamp;
begin
  if pv_log_level>=5 then
  dbms_output.put_line (pv_process_name||' log '||pv_log_level||' - '
                        ||to_char(v_timestamp,'HH24:MI:SS.FF3 :')||v_log_text);
  end if;
  INSERT INTO PLSQL_LOG(process_name
                       ,start_timestamp
                       ,log_timestamp
                       ,status
                       ,log_level
                       ,log_text
                       ,error_code)
  values (pv_process_name
         ,pv_executed_timestamp
         ,v_timestamp
         ,nvl(v_status,'I')
         ,pv_log_level
         ,v_log_text
         ,v_error_code
         );
  commit;
end write_log;
--
procedure write_plog(v_log_text   in varchar2
                   ,v_status     in varchar2 :=''
                   ,v_error_code in  varchar2 :=''
                   )
-- needs to be an autonomous transaction otherwise it would not be observable during execution and would potentially be
-- lost in a controlled rollback.
is
pragma autonomous_transaction;
v_timestamp      timestamp :=systimestamp;
begin
  if pv_log_level>=5 then
  dbms_output.put_line (pv_process_name||' log '||pv_log_level||' - '
                        ||to_char(v_timestamp,'HH24:MI:SS.FF3 :')||v_log_text);
  end if;
  INSERT INTO PROCESS_LOG(process_name
                         ,start_timestamp
                         ,log_timestamp
                         ,status
                         ,log_level
                         ,log_text
                         ,error_code)
  values (pv_process_name
         ,pv_executed_timestamp
         ,v_timestamp
         ,nvl(v_status,'I')
         ,pv_log_level
         ,v_log_text
         ,v_error_code
         );
  commit;
end write_plog;
--
procedure write_error(v_error_text in varchar2
                     ,v_error_code in varchar2 :=''
                     ,v_status     in varchar2 :=''
                     )
-- needs to be an autonomous transaction
is
pragma autonomous_transaction;
v_timestamp      timestamp :=systimestamp;
begin
  if pv_log_level>=3 then
  dbms_output.put_line (pv_process_name||' log '||pv_log_level||' - '
                        ||to_char(v_timestamp,'HH24:MI:SS.FF3 :')||v_error_text);
  end if;
  INSERT INTO process_error(process_name
                           ,start_timestamp
                           ,error_timestamp
                           ,status
                           ,error_text
                           ,error_code)
  values (pv_process_name
         ,pv_executed_timestamp
         ,v_timestamp
         ,nvl(v_status,'E')
         ,v_error_text
         ,v_error_code
         );
  commit;
  exception
  when others then
  dbms_output.put_line('CRITICAL the error handling subroutine write_error in '||pv_process_name||' failed');
  raise;
end write_error;
--
procedure get_prma (p_process_name in process_master.process_name%type
                   ,p_upd         in boolean :=true) is
-- mdw do I set the timestamp before I call this or within it?
begin
  -- only for testing, take these two out
  if pv_executed_timestamp is null then pv_executed_timestamp := systimestamp; 
  end if;  
  BEGIN
    select prma.* into pv_prma_rec
      from process_master prma
      where prma.process_name = upper(p_process_name);
  EXCEPTION
    WHEN no_data_found THEN
      INSERT INTO process_master (process_name, last_executed_timestamp, status, log_level, abandon_fl)
      VALUES(upper(p_process_name), pv_executed_timestamp, 'I', 5, 'N')
      RETURNING process_name, last_executed_timestamp, status, log_level, abandon_fl 
              INTO pv_prma_rec.process_name, 
                   pv_prma_rec.last_executed_timestamp, 
                   pv_prma_rec.status, 
                   pv_prma_rec.log_level, 
                   pv_prma_rec.abandon_fl;
  END;
  if p_upd then
    pv_prma_rec.status:='I';
    pv_prma_rec.last_executed_timestamp :=pv_executed_timestamp;    
    update process_master
    set status =pv_prma_rec.status
      ,last_executed_timestamp=pv_prma_rec.last_executed_timestamp
    where  process_name = p_process_name;
  end if;
  commit;
exception
  when no_data_found then
    -- cannot write to process_log or process_error as have no run to link to!
    -- so write to plsql_log
    pv_log_level :=5;
    pv_process_name :=nvl(pv_process_name,p_process_name);
    dbms_output.put_line('test');
    write_log ('Failed to find record in PROCESS_MASTER for '||p_process_name,'F','01403');
  raise;
end get_prma;
procedure upd_prma (p_prma_rec in   process_master%rowtype
                   ,p_upd      in varchar2 :='CS') is
pragma autonomous_transaction;
begin
  if p_upd='CS' then -- complete simple
    update process_master
    set status               = p_prma_rec.status
       ,stage                = p_prma_rec.stage
       ,abandon_fl           =p_prma_rec.abandon_fl
    where  process_name    = p_prma_rec.process_name;
  elsif p_upd='SU' then -- status_update t
    update process_master
    set status                 =pv_prma_rec.status
      ,last_executed_timestamp =pv_prma_rec.last_executed_timestamp
    where  process_name = p_prma_rec.process_name;
  elsif p_upd='WU' then -- window_update
    update process_master
    set status                  = p_prma_rec.status
       ,stage                   = p_prma_rec.stage
       ,window_start_timestamp1 = p_prma_rec.window_start_timestamp1
       ,window_start_id_num1    = p_prma_rec.window_start_id_num1
       ,window_end_timestamp1   = p_prma_rec.window_end_timestamp1
       ,window_end_id_num1      = p_prma_rec.window_end_id_num1       
    where  process_name         = p_prma_rec.process_name;
  elsif p_upd='CF' then -- close final (and also intermediate update
    update process_master
    set status               = p_prma_rec.status
       ,stage                = p_prma_rec.stage
       ,abandon_fl           = p_prma_rec.abandon_fl
       ,last_id_num1         = p_prma_rec.last_id_num1
       ,last_id_num2         = p_prma_rec.last_id_num2
       ,last_timestamp1      = p_prma_rec.last_timestamp1
    where  process_name    = p_prma_rec.process_name;    
  end if;
  commit;
end upd_prma;
--
procedure ins_prru (p_prru_rec    in process_run%rowtype) is
pragma autonomous_transaction;
begin
  -- only for testing, take these two out
  pv_executed_timestamp := systimestamp;
  pv_process_name :=p_prru_rec.process_name;
  insert into process_run
  values p_prru_rec;
  commit;
exception
  when others then
    pv_err_msg:=dbms_utility.format_error_stack;
    pv_err_stack := dbms_utility.format_error_backtrace;
    write_log(pv_err_msg,'E',SQLCODE);
    write_log(pv_err_stack,'E',SQLCODE);
    dbms_application_info.set_module(pv_init_module,pv_init_action);
    raise;
end ins_prru;
--
--
procedure upd_prru (p_prru_rec    in process_run%rowtype
                   ,p_upd         in varchar2 :='CS') is
pragma autonomous_transaction;
begin
  if p_upd='CS' then -- complete simple
    update process_run
    set status               = p_prru_rec.status
       ,completed_timestamp  = p_prru_rec.completed_timestamp
       ,records_processed    = p_prru_rec.records_processed
    where  process_name    = p_prru_rec.process_name
    and    start_timestamp = p_prru_rec.start_timestamp;
  elsif p_upd='WU' then
    update process_run
    set start_timestamp1     = p_prru_rec.start_timestamp1
       ,start_id_num1        = p_prru_rec.start_id_num1
       ,start_timestamp2     = p_prru_rec.start_timestamp2
       ,start_id_num2        = p_prru_rec.start_id_num2
       ,end_timestamp1       = p_prru_rec.end_timestamp1
       ,end_id_num1          = p_prru_rec.end_id_num1
       ,end_timestamp2       = p_prru_rec.end_timestamp2
       ,end_id_num2          = p_prru_rec.end_id_num2
    where  process_name    = p_prru_rec.process_name
    and    start_timestamp = p_prru_rec.start_timestamp;
  else -- update all the values. Generally you should only update all that needs updating
    update process_run
    set status               = p_prru_rec.status
       ,completed_timestamp  = p_prru_rec.completed_timestamp
       ,records_processed    = p_prru_rec.records_processed
       ,start_timestamp1     = p_prru_rec.start_timestamp1
       ,start_id_num1        = p_prru_rec.start_id_num1
       ,start_timestamp2     = p_prru_rec.start_timestamp2
       ,start_id_num2        = p_prru_rec.start_id_num2
       ,end_timestamp1       = p_prru_rec.end_timestamp1
       ,end_id_num1          = p_prru_rec.end_id_num1
       ,end_timestamp2       = p_prru_rec.end_timestamp2
       ,end_id_num2          = p_prru_rec.end_id_num2
       ,records_skipped      = p_prru_rec.records_skipped
       ,records_errored      = p_prru_rec.records_errored
    where  process_name    = p_prru_rec.process_name
    and    start_timestamp = p_prru_rec.start_timestamp;
  end if;
  commit;
exception
  when others then
    pv_err_msg:=dbms_utility.format_error_stack;
    pv_err_stack := dbms_utility.format_error_backtrace;
    write_plog(pv_err_msg,'E',SQLCODE);
    write_plog(pv_err_stack,'E',SQLCODE);
    write_error(pv_err_msg,sqlcode,'F');
    dbms_application_info.set_module(pv_init_module,pv_init_action);
    raise;
end upd_prru;

--Two versions of gen_err, one to use with dbms_utility, one with utl_call_stack
procedure gen_err_1 is
v_n1  number;    v_n2  number;
begin
  pv_call_stack := dbms_utility.format_call_stack;
  dbms_output.put_line('where are we in our code stack?1');
  dbms_output.put_line(pv_call_stack);
  dbms_output.put_line (' should error now!');
  v_n1:=0;
  v_n2:=100/v_n1;
end gen_err_1;
--
procedure gen_err2 is
v_n1  pls_integer;
v_n2  pls_integer;
v_stack_d pls_integer;
begin
  pv_call_stack := dbms_utility.format_call_stack;
  dbms_output.put_line('where are we in our code stack?');
--  dbms_output.put_line(pv_call_stack);
  dbms_output.put_line(' lv  Line        Owner unit_name');
  dbms_output.put_line('--- ----- ------------ --------------------------------');
  v_stack_d := utl_call_stack.dynamic_depth;
  for i in reverse 1..v_stack_d loop
    dbms_output.put_line(to_char(i,'99')||' '
      ||to_char(utl_call_stack.unit_line(i),'99999')||' '
      ||lpad(nvl(utl_call_stack.owner(i),' '),12)||' '
      ||lpad(utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(i)),' ',30));
  end loop;
  dbms_output.put_line (' should error now!');
  v_n1:=0;
  v_n2:=100/v_n1;
end gen_err2;
procedure gen_err is
v_n1  pls_integer;
v_n2  pls_integer;
v_stack_d pls_integer;
begin
  pv_call_stack := dbms_utility.format_call_stack;
  dbms_output.put_line('where are we in our code stack?');
--  dbms_output.put_line(pv_call_stack);
  dbms_output.put_line('    loc');
  dbms_output.put_line(' lv lev  Line        Owner unit_name                        Edition');
  dbms_output.put_line('--- --- ----- ------------ -------------------------------- ---------');
  v_stack_d := utl_call_stack.dynamic_depth;
  for i in 1..v_stack_d loop
    dbms_output.put_line(to_char(i,'99')||' '||to_char(utl_call_stack.lexical_depth(i),'99')
      ||to_char(utl_call_stack.unit_line(i),'99999')||' '
      ||lpad(nvl(utl_call_stack.owner(i),' '),12)||' '
      ||rpad(utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(i)),32)
      ||nvl(utl_call_stack.current_edition(i),' cur'));
  end loop;
--  dbms_output.put_line (' should error now!');
  v_n1:=0;
--  v_n2:=100/v_n1;
end gen_err;
--
procedure test1 is
v_vc1 varchar2(100);
begin
  pv_process_name       :='test1';
  pv_log_level          := 5;
  pv_executed_timestamp := systimestamp;
  dbms_application_info.set_module(module_name => 'PNA_TEST',action_name =>'START');
  pl('started code');
  -- do stuff
  v_vc1 :=piece('eric*the*red',3,'*');
  pl('ended code');
  gen_err;
  dbms_application_info.set_module(module_name => '',action_name =>'');
--exception
--  when others then
--    pv_err_msg:=dbms_utility.format_error_stack;
--    pv_err_stack := dbms_utility.format_error_backtrace;
--    write_log(pv_err_msg,'E',SQLCODE);
--    write_log(pv_err_stack,'E',SQLCODE);
--    raise;
end test1;
--
procedure test_auto is
-- the most basic test of automating a task
v_count pls_integer;
begin
  pv_process_name       :='test_auto';
  pv_log_level          := 5;
  pv_executed_timestamp := systimestamp;
  pv_prma_rec:=null;
  pv_prru_rec:=null;
  get_prma('TEST_AUTO');
  dbms_output.put_line ('name '||pv_prma_rec.process_name||'  timestamp '||to_char(pv_prma_rec.last_executed_timestamp));
  pv_prru_rec.process_name       := pv_prma_rec.process_name;
  pv_prru_rec.start_timestamp    := pv_prma_rec.last_executed_timestamp;
  pv_prru_rec.status             := pv_prma_rec.status;
  pv_prru_rec.log_level          := pv_prma_rec.log_level;
  ins_prru(pv_prru_rec);
  -- the work of the procedure now begins
  select count(*) into v_count from person;
  dbms_lock.sleep(10);
  pv_prru_rec.records_processed :=v_count;
  pv_prru_rec.status               :='C';
  pv_prru_rec.completed_timestamp  :=systimestamp;
  upd_prru(pv_prru_rec,'CS');
  pv_prma_rec.status :='C';
  pv_prma_rec.stage  :=null;
  upd_prma(pv_prma_rec,'CS');
--
  write_plog ('ended at '||to_char(systimestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  dbms_application_info.set_module(pv_init_module,pv_init_action);
end test_auto;
--
procedure pop_source(p_log_level in pls_integer := 5)
is
-- populate the fn, sn, and addr source tables.
-- I'll create an array and put into it the info I want to shove into the tables.
type name_varray is varray (100) of varchar2(4000);
sn_varray name_varray :=name_varray();
fn_varray name_varray :=name_varray();
rn_varray name_varray :=name_varray();
v_text          varchar2(30);
v_count         pls_integer;
begin
  -- Rather than have a long list of insert statements, I put the raw data into a set of delimited strings
  -- and process them in a nested loop that inserts each value.
  dbms_application_info.read_module(pv_init_module,pv_init_action);
  dbms_application_info.set_module(module_name => 'POP_SOURCE',action_name =>'SETUP');
  pv_process_name       :='POP_SOURCE';
  pv_log_level          := p_log_level;
  pv_executed_timestamp := systimestamp;
  write_log ('started at '||to_char(pv_executed_timestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  select count(*) into v_count from sn;
  if v_count>0 then
      select count(*) into v_count from all_tables;
      dbms_output.put_line ('whoops!');
  else
    -- populate the SN table
    dbms_application_info.set_action('POP_SN');
    write_log ('populating surnames');
    -- When I create the example tables PERSON, PERSON_NAME and ADDRESS I want there
    -- to be a psuedo-realistic spread of names, with some more common than others
    -- An easy way to do this is to create the source tables with a varying number of entries per
    -- name, achieved by using the array index number.
    -- eg there will be 10 SMITHs, 6 from array entry 6 and 4 from array entry 4.
    sn_varray.extend(6);
    sn_varray(1) := 'ALDERTON*BENSON*MACDONALD*MCDONALD*O''DRISCOLL*DENT*INGRAMS*NASH*FAIRBURN'
                   ||'*LAU*DAS*SUZUKI*TANAKA*YAMAMOTO*PREFECT*MCMILLAN*WIDLAKE*ZHU';
    sn_varray(2):= 'BARKER*CHURCHILL*COSTA*COX*DIAZ*DOHERTY*EVERTON*FERNANDEZ*GREEN*GOMES*JOHANSEN'
                   ||'*O''NEIL*SANTOS*SILVA*STUART*WATSON*YOUNG';
    sn_varray(3) :='ALLEN*BAKER*BLACK*BROWN*CLARKE*GARCIA*HARRIS*HANSEN*JONES*PHILLIPS*ROSE'
                   ||'*RAMIREZ*RHODES*ROBERTS*SAMSON*SMYTH*STEWART';
    sn_varray(4) := 'ADAMS*ANDERSON*HERNANDEZ*LEE*ORPINGTON-SMYTH*SCOTT*SMITH*TAYLOR*THOMAS*WHITE';
    sn_varray(5) := 'BOUCHARD*LEBLANC*DAVIS*KHAN*KHUMAR*MARTINEZ*SINGH*YOUNG*WANG*WONG';
    sn_varray(6) := 'ALI*CHEN*MARTIN*MOHAMMED*JOHNSON*JONES*LEE*PATEL*SMITH*WILLIAMS*WILSON';
    for elm in 1..sn_varray.count loop
      if pv_log_level >5 then
        write_log('processing entry '||elm);
      end if;
      v_count:=1;
      loop
        v_text :=piece(sn_varray(elm),v_count,'*');
        exit when v_text is null;
        for i in 1..elm loop
          insert into sn (surn_id,surname) values (surn_seq.nextval,v_text);
        end loop;
        v_count:=v_count+1;
        if v_count >100 then exit; -- just in case I mess something up...
        end if;
      end loop;
    end loop;
    commit;
    dbms_application_info.set_action('POP_FN');
    write_log ('populating forenames');
    fn_varray.extend(5);
    fn_varray(1) := 'AUGUSTIN*BARRY*CHENG*DUNCAN*ERIC*GARY*HENRY*HAROLD*JADEN*JANG**KRIS*LARRY*LUIS*MARTYN*NOAH*'
                   ||'FENG*JIE*MARMADUKE*PENG*SANTIAGO*SEBASTIAN*SEBASTIEN*VALERIE*VADIM*VLADIMIR*WEI';
    fn_varray(2):= 'ALBERT*CHRIS*COLLIN*CHRISTIAN*DIPAK*ETHAN*JACK*HARRY*KEVIN*JAN*MANUAL*MARTIN*MENENDEZ*'
                   ||'NICHOLAS*OLIVER*OWEN*RICK*SAM*SAMUEL*STEPHEN*STEVEN*TERRY*WILLIAM';
    fn_varray(3) :='ALI*ALAN*ARON*ANDREW*ADAM*BEN*CARTER*CHRISTOPHER*EDWARD*IAN*LIAM*MIGUEL*'
                   ||'NEIL*NOEL*JOHN*MOHAMMED*PATRICK*PAUL*PETE*ROGER*WILLIAM*YOUSSEF';
    fn_varray(4) := 'ADAM*ALEXANDER*BEN*BENJAMIN*CHARLES*JACK*JACOB*JAMES*JON*JOSE*JUAN*OLIVER*PETER*RICHARD';
    fn_varray(5) := 'AHMED*CHARLIE*DAVE*DAVID*JOHN*JOSEPH*OLIVER*MIGUEL*MOHAMMED*WILLIAM';
    for elm in 1..fn_varray.count loop
      if pv_log_level >5 then
        write_log('processing entry '||elm);
      end if;
      v_count:=1;
      loop
        v_text :=piece(fn_varray(elm),v_count,'*');
        exit when v_text is null;
        for i in 1..elm loop
          insert into fn (forn_id,forename,sex_ind) values (forn_m_seq.nextval,v_text,'M');
        end loop;
        v_count:=v_count+1;
        if v_count >100 then exit; -- just in case I mess something up...
        end if;
      end loop;
    end loop;
--
    fn_varray(1) := 'ABBY*ANGEL*ANNIE*BABS*BARBIE*CHERRY*CHERIE*CHERYL*ELOISE*HILDA*GEMMA*JAY*LI*NORA*'
                   ||'MARTINA*OLA*ONA*PATTY*POPPY*SANDY*TING*XIAN*ZAHRA*ZARA';
    fn_varray(2):= 'ABIGAIL*BARBARA*ELLIE*CLARA*DAISY*FRAN*FRANCIS*INGRID*JANET*JESSICA*KAREN*LARA*LEYLA*'
                   ||'PETRA*PAULINE*SARA*SALLY*SIAN*SOPHIE*SOPHIA*STEPHANIE*YASMIN';
    fn_varray(3) :='AMELIA*ANYA*EMMA*ELAINE*GILLIAN*LOUISE*MARY*AYA*FATMA*HANNAH*KATE*LUCIE*MADISON*'
                   ||'*NATALIE*NICKY*POPPY*SANDRA*SAMANTHA*VALENTINA*VALERIE*WENDY';
    fn_varray(4) := 'AVA*ALISON*BARABRA*CHLOE*DAWN*EMMA*JULIA*LUCY*NICOLA*RACHAEL*SOFIA*TINA*';
    fn_varray(5) := 'ANN*ANNA*EMILY*FATIMA*ISABEL*JANE*JULIE*KATE*LINDA*MARIA*MARY*OLIVIA*SARAH*SUE*SUSAN*';
    for elm in 1..fn_varray.count loop
      if pv_log_level >5 then
        write_log('processing entry '||elm);
      end if;
      v_count:=1;
      loop
        v_text :=piece(fn_varray(elm),v_count,'*');
        exit when v_text is null;
        for i in 1..elm loop
          insert into fn (forn_id,forename,sex_ind) values (forn_m_seq.nextval,v_text,'F');
        end loop;
        v_count:=v_count+1;
        if v_count >100 then exit; -- just in case I mess something up...
        end if;
      end loop;
    end loop;
    commit;
--
    dbms_application_info.set_action('POP_ROAD_NAME_TYPE');
    write_log ('populating road names and types');
    rn_varray.extend(4);
    rn_varray(1) := 'ST PETERS*CHERRY*PEAR TREE*APPLE YARD*JOHNS*TUCKER*COLBY*ANDREW*DIMBLES*WIDDINGHAM*'
                   ||'BRISTOL*LONDON*MAIN*ERDINGTON*COTHAM*MAPLETHORPE*SEA VIEW*BLACK FRIARS*UPPER*LOWER';
    rn_varray(2) := 'TALBOT*MOOR ALLERTON*LARKHILL*CHANDOS*LINCOMBE*GREEN*MOORLAND*FALKLAND*KING*MALDEN*QUEEN*'
                   ||'PRINCE*PRINCE ALBERT*PARKSIDE*EATON*HOLT*SHEPCOTE*OTLEY*MOSLEY*KIRKWOOD*ARGYL*WOOD';
    rn_varray(3) :='NEW*OLD*ROSE*BARLEY*REGENT*CRAIGWOOD*CRAGG*KERRY*QUEENS*ROYAL PARK*BRUNDELL*'
                   ||'ASH*WILLOW*ASHVILLE*EASTON*ALEXANDRA*PARK*WESTFIELD*SAINT JOHNS*CLARENDON*MOORLAND';
    rn_varray(4) := 'KIRKSTALL*BELLE VUE*FESTIVAL*SAINT MARKS*SAINTS*CARLTON*LEYTHE*GLOUCESTER*'
                   ||'BERRYMEAD*ALL SAINTS*GREENEND*ST ELMO*STANSFIELD*PERCY*DEVENPORT*GOLDHAWK*WESTIVILLE*SYCAMORE';
    for elm in 1..rn_varray.count loop
      if pv_log_level >5 then
        write_log('processing entry '||elm);
      end if;
      v_count:=1;
      loop
        v_text :=piece(rn_varray(elm),v_count,'*');
        exit when v_text is null;
        insert into road_name (rona_id,road_name) values (rona_seq.nextval,v_text);
        v_count:=v_count+1;
        if v_count >100 then exit; -- just in case I mess something up...
        end if;
      end loop;
    end loop;
    rn_varray.extend(1);
    rn_varray(1) := 'MOUNT*PARADE*APPROACH*HILL CRESENT*HILL DRIVE*HILL ROAD*'
                   ||'BANK*VALE ROAD*VALE*NORTH AVENUE*NORTH TERRACE*SOUTH TERRACE*'
                   ||'TERRACE*RIDINGS*SOUTH*NORTH*EAST*WEST*NORTH DRIVE*SOUTH DRIVE*BANK DRIVE*HEIGHTS';
    rn_varray(2) := 'VIEW**PARK*COURT*GARDEN*PLACE*MOUNT*GROVE*RISE*VIEW';
    rn_varray(3) := 'WAY*HILL*GARDENS*DRIVE*STREET';
    rn_varray(4) := 'CLOSE*LANE*DRIVE*ROAD*STREET';
    rn_varray(5) := 'ROAD*STREET*AVENUE';
    for elm in 1..rn_varray.count loop
      if pv_log_level >5 then
        write_log('processing entry '||elm);
      end if;
      v_count:=1;
      loop
        v_text :=piece(rn_varray(elm),v_count,'*');
        exit when v_text is null;
        for i in 1..elm loop
          insert into road_type(roty_id,road_type) values (roty_seq.nextval,v_text);
        end loop;
        v_count:=v_count+1;
        if v_count >100 then exit; -- just in case I mess something up...
        end if;
      end loop;
    end loop;
    -- for the town names, there is little to gain by staging the more complex data in delimited strings
    -- and I want only 1 entry per "area". So a set of simple insert statments is the better solution
    insert into town_name values (tona_seq.nextval,'','HARROGATE','YORKSHIRE','HG2');
    insert into town_name values (tona_seq.nextval,'','LEEDS','YORKSHIRE','LS1');
    insert into town_name values (tona_seq.nextval,'BURLEY','LEEDS','YORKSHIRE','LS8');
    insert into town_name values (tona_seq.nextval,'YEADON','LEEDS','YORKSHIRE','LS19');
    insert into town_name values (tona_seq.nextval,'','LICHFIELD','STAFFORDSHIRE','WS13');
    insert into town_name values (tona_seq.nextval,'ATTERCLIFFE','SHEFFIELD','YORKSHIRE','SH2');
    insert into town_name values (tona_seq.nextval,'PARSON CROSS','SHEFFIELD','YORKSHIRE','SH5');
    insert into town_name values (tona_seq.nextval,'COTHAM','BRISTOL','AVON','BS6');
    insert into town_name values (tona_seq.nextval,'CLIFTON','BRISTOL','AVON','BS8');
    insert into town_name values (tona_seq.nextval,'KEYNSHAM','BRISTOL','AVON','BS18');
    insert into town_name values (tona_seq.nextval,'','LICHFIELD','STAFFORDSHIRE','WS14');
    insert into town_name values (tona_seq.nextval,'BINGHAM','NOTTINGHAM','NOTTS','NG13');
    insert into town_name values (tona_seq.nextval,'CLIFTON','NOTTINGHAM','NOTTS','NG7');
    insert into town_name values (tona_seq.nextval,'LONG EATON','NOTTINGHAM','NOTTS','NG11');
    insert into town_name values (tona_seq.nextval,'BASFORD','NOTTINGHAM','NOTTS','NG6');
    insert into town_name values (tona_seq.nextval,'HORSFORTH','LEEDS','YORKSHIRE','LS18');
    insert into town_name values (tona_seq.nextval,'','HITCHIN','HERTFORDSHIRE','SG2');
    insert into town_name values (tona_seq.nextval,'','READING','BERKSHIRE','RG2');
    insert into town_name values (tona_seq.nextval,'','BRACKNELL','BERKSHIRE','RG4');
    insert into town_name values (tona_seq.nextval,'','BRIDLINGTON','EAST YORKS','YO16');
    insert into town_name values (tona_seq.nextval,'HEADINGLEY','LEEDS','YORKSHIRE','LS16');
    insert into town_name values (tona_seq.nextval,'COOKRIDGE','LEEDS','YORKSHIRE','LS16');
    insert into town_name values (tona_seq.nextval,'HORSEFORTH','LEEDS','YORKSHIRE','LS16');
    insert into town_name values (tona_seq.nextval,'WEETWOOD','LEEDS','YORKSHIRE','LS6');
    insert into town_name values (tona_seq.nextval,'YEADON','LEEDS','YORKSHIRE','LS20');
    insert into town_name values (tona_seq.nextval,'GUISLEY','LEEDS','YORKSHIRE','LS20');
    insert into town_name values (tona_seq.nextval,'','GUISLEY','YORKSHIRE','LS20');
    insert into town_name values (tona_seq.nextval,'ALWOODLEY','LEEDS','YORKSHIRE','LS19');
    insert into town_name values (tona_seq.nextval,'','LEEDS','YORKSHIRE','LS2');
    insert into town_name values (tona_seq.nextval,'','LEEDS','YORKSHIRE','LS3');
    insert into town_name values (tona_seq.nextval,'','LEEDS','YORKSHIRE','LS4');
    insert into town_name values (tona_seq.nextval,'','LEEDS','YORKSHIRE','LS5');
    insert into town_name values (tona_seq.nextval,'BURLEY','LEEDS','YORKSHIRE','LS6');
    insert into town_name values (tona_seq.nextval,'','LEEDS','YORKSHIRE','LS7');
    insert into town_name values (tona_seq.nextval,'','LEEDS','YORKSHIRE','LS8');
    insert into town_name values (tona_seq.nextval,'MORLEY','LEEDS','YORKSHIRE','LS27');
    insert into town_name values (tona_seq.nextval,'','BATLEY','YORKSHIRE','LS27');
    insert into town_name values (tona_seq.nextval,'','YORK','YORKSHIRE','Y01');
    insert into town_name values (tona_seq.nextval,'','YORK','YORKSHIRE','YO2');
    insert into town_name values (tona_seq.nextval,'TOCKWITH','YORK','YORKSHIRE','YO26');
    insert into town_name values (tona_seq.nextval,'UPPER POPPLETON','YORK','YORKSHIRE','YO26');
    insert into town_name values (tona_seq.nextval,'LOWER POPPLETON','YORK','YORKSHIRE','YO26');
    insert into town_name values (tona_seq.nextval,'RAWCLIFFE','YORK','YORKSHIRE','YO30');
    insert into town_name values (tona_seq.nextval,'HUNTINGTON','YORK','YORKSHIRE','YO32');
    insert into town_name values (tona_seq.nextval,'STAMFORD BRIDGE','YORK','YORKSHIRE','YO41');
    insert into town_name values (tona_seq.nextval,'','STAMFORD BRIDGE','YORKSHIRE','YO41');
    insert into town_name values (tona_seq.nextval,'DUNNINGTON','YORK','YORKSHIRE','YO19');
    insert into town_name values (tona_seq.nextval,'','PONTEFRACT','YORKSHIRE','WF8');
    insert into town_name values (tona_seq.nextval,'','CASTLEFORD','YORKSHIRE','WF10');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD3');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD2');
    insert into town_name values (tona_seq.nextval,'ECCLESHILL','BRADFORD','YORKSHIRE','BD2');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD17');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD5');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD7');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD1');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD8');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD13');
    insert into town_name values (tona_seq.nextval,'','BRADFORD','YORKSHIRE','BD15');
    insert into town_name values (tona_seq.nextval,'','SHIPLEY','YORKSHIRE','BD18');
    insert into town_name values (tona_seq.nextval,'','MORLEY','YORKSHIRE','LS19');
    insert into town_name values (tona_seq.nextval,'','MORLEY','YORKSHIRE','LS27');
    insert into town_name values (tona_seq.nextval,'','HOLMFIRTH','YORKSHIRE','HD7');
    insert into town_name values (tona_seq.nextval,'','PONTEFRACT','YORKSHIRE','WF8');
    insert into town_name values (tona_seq.nextval,'','TODMORDEN','YORKSHIRE','OL14');
    insert into town_name values (tona_seq.nextval,'','COLLINGHAM','YORKSHIRE','LS22');
    insert into town_name values (tona_seq.nextval,'COLLINGHAM','LEEDS','YORKSHIRE','LS2');
    insert into town_name values (tona_seq.nextval,'','HEBDEN BRIDGE','YORKSHIRE','HX7');
    insert into town_name values (tona_seq.nextval,'','HECKMONDWIKE','YORKSHIRE','WF16');
--
  end if; -- opt out if data already existed.
  write_log ('Ended');
  dbms_application_info.set_module(pv_init_module,pv_init_action);
exception
  when others then
    pv_err_msg:=dbms_utility.format_error_stack;
    pv_err_stack := dbms_utility.format_error_backtrace;
    write_log(pv_err_msg,'E',SQLCODE);
    write_log(pv_err_stack,'E',SQLCODE);
    dbms_application_info.set_module(pv_init_module,pv_init_action);
    raise;
end pop_source;
--
procedure pop_addr_rbyr(p_rows   in pls_integer :=1000000) is
v_tona_min    pls_integer;
v_roty_min    pls_integer;
v_rona_min    pls_integer;
v_tona_max    pls_integer;
v_roty_max    pls_integer;
v_rona_max    pls_integer;
v_tona_sd     pls_integer;
v_roty_sd     pls_integer;
v_rona_sd     pls_integer;
v_house       pls_integer;
v_vc1         varchar2(100);
v_rona        varchar2(20);
v_roty        varchar2(15);
v_addr_rec    town_name%rowtype;
begin
  -- This code finds the min and max IDs for road type, name and town but DOES assume it is a contiguous set
  -- which is *fairly* safe as they are populated in a single procedure with no cache on the sequences
  -- I should re-write this to use collections!
  dbms_application_info.read_module(pv_init_module,pv_init_action);
  dbms_application_info.set_module(module_name => 'PO_ADDR_RBYR',action_name =>'START');
  pv_process_name       :='POP_ADDR_RBYR';
  pv_log_level          := 5;
  pv_executed_timestamp := systimestamp;
  write_log ('started at '||to_char(pv_executed_timestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  select min(tona_id),max(tona_id) into v_tona_min,v_tona_max from town_name;
  select min(rona_id),max(rona_id) into v_rona_min,v_rona_max from road_name;
  select min(roty_id),max(roty_id) into v_roty_min,v_roty_max from road_type;
  write_log ('towns '||to_char(v_tona_min)||' to '||to_char(v_tona_max)
         ||'  roads '||to_char(v_rona_min)||' to '||to_char(v_rona_max)
         ||'  types '||to_char(v_roty_min)||' to '||to_char(v_roty_max) );
  -- Now create as many addresses as asked (default 1 million
  dbms_application_info.set_action('main address loop');
  for a in 1..p_rows loop
    v_tona_sd := trunc(dbms_random.value(v_tona_min,v_tona_max+1));
    v_rona_sd := trunc(dbms_random.value(v_rona_min,v_rona_max+1));
    v_roty_sd := trunc(dbms_random.value(v_roty_min,v_roty_max+1));
    select tona_id,area_name,town_name,town_county,post_code
    into v_addr_rec
    from town_name
    where tona_id = v_tona_sd;
    select road_name into v_rona
    from road_name
    where rona_id = v_rona_sd;
    select road_type into v_roty
    from road_type
    where roty_id = v_roty_sd;
    v_vc1 :=v_addr_rec.post_code||' '||
              to_char(trunc(dbms_random.value(1,10)))
              ||dbms_random.string('u',2);
    insert into address (addr_id
                        ,house_number
                        ,addr_line_1
                        ,addr_line_2
                        ,addr_line_3
                        ,addr_line_4
                        ,post_code)
    values (addr_seq.nextval
           ,decode (  trunc(dbms_random.value(0,4  ))
                   ,0,trunc(dbms_random.value(1,61 ))
                   ,1,trunc(dbms_random.value(1,121))
                   ,2,trunc(dbms_random.value(1,121))
                   ,  trunc(dbms_random.value(1,100)) )
           ,v_rona||' '||v_roty
           ,v_addr_rec.area_name
           ,v_addr_rec.town_name
           ,v_addr_rec.town_county
           ,v_vc1
           );
    if mod(a,10000) = 0 then
      -- dbms_lock.sleep(5);
      write_log ('intermediate commit at '||a||' addresses');
      commit;
    end if;
  end loop;
  write_log ('ended at '||to_char(systimestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  write_log ('elapsed is '||substr(systimestamp-pv_executed_timestamp,10,20));
  commit;
  dbms_application_info.set_module(pv_init_module,pv_init_action);
exception
  when others then
    pv_err_msg:=dbms_utility.format_error_stack;
    pv_err_stack := dbms_utility.format_error_backtrace;
    write_log(pv_err_msg,'E',SQLCODE);
    write_log(pv_err_stack,'E',SQLCODE);
    dbms_application_info.set_module(pv_init_module,pv_init_action);
    raise;
end pop_addr_rbyr;
--
procedure pop_addr_batch(p_rows      in pls_integer :=1000000
                        ,p_log_level in pls_integer :=5) is
v_tona_min    pls_integer;
v_roty_min    pls_integer;
v_rona_min    pls_integer;
v_tona_max    pls_integer;
v_roty_max    pls_integer;
v_rona_max    pls_integer;
v_count       pls_integer;
loop_count    pls_integer;
v_chunk_size pls_integer :=10000;
v_addr_rec town_name%rowtype;
type addr_elements is record (rn pls_integer
                             ,rt pls_integer
                             ,tn pls_integer
                             ,hn pls_integer
                             ,pcn pls_integer
                             ,pcvc varchar2(2)
                             );
type array_addr is table of addr_elements index by binary_integer;
addr_array   array_addr;
procedure chk_addr_count is
v_num_addr      pls_integer;
begin
    select count(*) into v_num_addr from address;
    write_log('number of addresses now '||v_num_addr);
end chk_addr_count;
begin
  -- This code finds the min and max IDs for road type, name and town but DOES assume it is a contiguous set
  -- which is *fairly* safe as they are populated in a single procedure with no cache on the sequences
  dbms_application_info.read_module(pv_init_module,pv_init_action);
  dbms_application_info.set_module(module_name => 'POP_ADDR_BATCH',action_name =>'START');
  pv_process_name       :='POP_ADDR_BATCH';
  pv_log_level          := p_log_level;
  pv_executed_timestamp := systimestamp;
  write_log ('started at '||to_char(pv_executed_timestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  select min(tona_id),max(tona_id) into v_tona_min,v_tona_max from town_name;
  select min(rona_id),max(rona_id) into v_rona_min,v_rona_max from road_name;
  select min(roty_id),max(roty_id) into v_roty_min,v_roty_max from road_type;
  write_log ('towns '||to_char(v_tona_min)||' to '||to_char(v_tona_max)
         ||'  roads '||to_char(v_rona_min)||' to '||to_char(v_rona_max)
         ||'  types '||to_char(v_roty_min)||' to '||to_char(v_roty_max) );
  -- Now create as many addresses as asked (default 1 million)
  v_count :=0;
  loop_count :=0;
  while v_count < p_rows loop
    for a in 1..v_chunk_size loop
      addr_array(a).tn := trunc(dbms_random.value(v_tona_min,v_tona_max+1));
      addr_array(a).rn := trunc(dbms_random.value(v_rona_min,v_rona_max+1));
      addr_array(a).rt := trunc(dbms_random.value(v_roty_min,v_roty_max+1));
      addr_array(a).hn := case trunc(dbms_random.value(0,4  ))
                          when 0 then trunc(dbms_random.value(1,61 ))
                          when 1 then trunc(dbms_random.value(1,121))
                          when 2 then trunc(dbms_random.value(1,121))
                          else   trunc(dbms_random.value(1,100))
                          end;
      addr_array(a).pcn := trunc(dbms_random.value(1,10));
      addr_array(a).pcvc := dbms_random.string('u',2);
    end loop;
    forall idx in indices of addr_array
    insert into address (addr_id
                        ,house_number
                        ,addr_line_1
                        ,addr_line_2
                        ,addr_line_3
                        ,addr_line_4
                        ,post_code)
    select addr_seq.nextval
          ,addr_array(idx).hn
          ,rona.road_name||' '||roty.road_type
          ,tona.area_name
          ,tona.town_name
          ,tona.town_county
          ,tona.post_code||' '|| to_char(addr_array(idx).pcn)
                            ||addr_array(idx).pcvc
    from  town_name tona
         ,road_name rona
         ,road_type roty
    where tona.tona_id = addr_array(idx).tn
    and   rona.rona_id = addr_array(idx).rn
    and   roty.roty_id = addr_array(idx).rt;
    v_count:=v_count+v_chunk_size;
    loop_count:=loop_count+1;
    if mod(loop_count,10)=0 then
      commit;
      write_log ('intermediate commit at '||v_count||' addresses');
    end if;
  end loop; -- main p_rows insert
--  dbms_trace.set_plsql_trace(dbms_trace.trace_all_calls
--                            +dbms_trace.trace_all_SQL);
--  if pv_log_level >5 then
--    chk_addr_count;
--  end if;
  write_log ('ended at '||to_char(systimestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  write_log ('elapsed is '||substr(systimestamp-pv_executed_timestamp,10,20));
--  dbms_trace.set_plsql_trace(dbms_trace.trace_stop);
  commit;
  dbms_application_info.set_module(pv_init_module,pv_init_action);
exception
  when others then
    pv_err_msg:=dbms_utility.format_error_stack;
    pv_err_stack := dbms_utility.format_error_backtrace;
    write_log(pv_err_msg,'E',SQLCODE);
    write_log(pv_err_stack,'E',SQLCODE);
    dbms_application_info.set_module(pv_init_module,pv_init_action);
    raise;
end pop_addr_batch;
--
procedure pop_addr_h is
v_tona_min    pls_integer;
v_roty_min    pls_integer;
v_rona_min    pls_integer;
v_tona_max    pls_integer;
v_roty_max    pls_integer;
v_rona_max    pls_integer;
v_count       pls_integer;
v_addr_rec town_name%rowtype;
type addr_elements is record (rn pls_integer
                             ,rt pls_integer
                             ,tn pls_integer
                             ,hn pls_integer
                             ,pcn pls_integer
                             ,pcvc varchar2(2)
                             );
type array_addr is table of addr_elements index by binary_integer;
addr_array   array_addr;
begin
  -- This code finds the min and max IDs for road type, name and town but DOES assume it is a contiguous set
  -- which is *fairly* safe as they are populated in a single procedure with no cache on the sequences
  dbms_application_info.read_module(pv_init_module,pv_init_action); 
  pv_process_name       :='POP_ADDR_H';
  pv_log_level          :=5;
  dbms_application_info.set_module(module_name => pv_process_name,action_name =>'START');
  pv_executed_timestamp := systimestamp;
  pv_prma_rec:=null;  pv_prru_rec:=null;
  write_log('collecting and updating the process_master for '||pv_process_name);
  get_prma(pv_process_name);
  pv_log_level               := pv_prma_rec.log_level;
  pv_prru_rec.process_name       := pv_prma_rec.process_name;
  pv_prru_rec.start_timestamp    := pv_prma_rec.last_executed_timestamp;
  pv_prru_rec.status             := pv_prma_rec.status;
  pv_prru_rec.log_level          := pv_prma_rec.log_level;
  pv_prru_rec.process_range      := pv_prma_rec.process_range;
  pv_prru_rec.batch_size         := pv_prma_rec.batch_size;
  pv_prru_rec.records_processed  := 0;
  pv_prru_rec.records_skipped    := 0;
  pv_prru_rec.records_errored    := 0;
  write_log('creating the process_run entry and updating');
  ins_prru(pv_prru_rec);
  write_plog ('started at '||to_char(pv_executed_timestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  if pv_prma_rec.abandon_fl = 'Y' then
    write_plog('process set to Abandon so halted');
  else
  select min(tona_id),max(tona_id) into v_tona_min,v_tona_max from town_name;
  select min(rona_id),max(rona_id) into v_rona_min,v_rona_max from road_name;
  select min(roty_id),max(roty_id) into v_roty_min,v_roty_max from road_type;
  write_plog ('towns '||to_char(v_tona_min)||' to '||to_char(v_tona_max)
         ||'  roads '||to_char(v_rona_min)||' to '||to_char(v_rona_max)
         ||'  types '||to_char(v_roty_min)||' to '||to_char(v_roty_max) );
  -- Now create as many addresses as asked (default 1 million)
  v_count :=0;
  dbms_application_info.set_action('Running');
  while v_count < pv_prma_rec.process_range loop
    for a in 1..pv_prma_rec.batch_size loop
      addr_array(a).tn := trunc(dbms_random.value(v_tona_min,v_tona_max+1));
      addr_array(a).rn := trunc(dbms_random.value(v_rona_min,v_rona_max+1));
      addr_array(a).rt := trunc(dbms_random.value(v_roty_min,v_roty_max+1));
      addr_array(a).hn := case trunc(dbms_random.value(0,4  ))
                          when 0 then trunc(dbms_random.value(1,61 ))
                          when 1 then trunc(dbms_random.value(1,121))
                          when 2 then trunc(dbms_random.value(1,121))
                          else   trunc(dbms_random.value(1,100))
                          end;
      addr_array(a).pcn := trunc(dbms_random.value(1,10));
      addr_array(a).pcvc := dbms_random.string('u',2);
    end loop;
    forall idx in indices of addr_array
    insert into address (addr_id
                        ,house_number
                        ,addr_line_1
                        ,addr_line_2
                        ,addr_line_3
                        ,addr_line_4
                        ,post_code)
    select addr_seq.nextval
          ,addr_array(idx).hn
          ,rona.road_name||' '||roty.road_type
          ,tona.area_name
          ,tona.town_name
          ,tona.town_county
          ,tona.post_code||' '|| to_char(addr_array(idx).pcn)
                            ||addr_array(idx).pcvc
    from  town_name tona
         ,road_name rona
         ,road_type roty
    where tona.tona_id = addr_array(idx).tn
    and   rona.rona_id = addr_array(idx).rn
    and   roty.roty_id = addr_array(idx).rt;
    v_count:=v_count+pv_prma_rec.batch_size;
      dbms_lock.sleep(10); -- just so it takes some time
      commit;
      write_plog ('intermediate commit at '||v_count||' addresses');
  end loop; -- main p_rows insert
  commit;
  -- now update the run and master
  pv_prru_rec.records_processed    :=v_count;
  end if; -- abandon_check
  if pv_prma_rec.abandon_fl='Y' then
    pv_prru_rec.status     :='A';   pv_prma_rec.status     :='A';
  else
    pv_prru_rec.status     :='C';   pv_prma_rec.status     :='C';
  end if;  
  pv_prru_rec.completed_timestamp  :=systimestamp;
  upd_prru(pv_prru_rec,'CS');
  pv_prma_rec.stage  :=null;
  upd_prma(pv_prma_rec,'CS');
  write_plog ('ended at '||to_char(systimestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  dbms_application_info.set_module(pv_init_module,pv_init_action);
exception
  when others then
    pv_err_msg:=dbms_utility.format_error_stack;
    pv_err_stack := dbms_utility.format_error_backtrace;
    write_plog(pv_err_msg,'E',SQLCODE);
    write_plog(pv_err_stack,'E',SQLCODE);
    write_error(pv_err_msg,sqlcode,'F');
    -- update the control tables to show it errored
    pv_prru_rec.status               :='E';
    pv_prru_rec.completed_timestamp  :=systimestamp;
    upd_prru(pv_prru_rec,'CS');
    pv_prma_rec.status :='E';
    pv_prma_rec.stage  :=null;
    pv_prma_rec.abandon_fl :='Y';
    upd_prma(pv_prma_rec,'CS');
    dbms_application_info.set_module(pv_init_module,pv_init_action);
    raise;
end pop_addr_h;
--
procedure pop_pers(p_families    in pls_integer :=100000
                  ,p_log_level   in pls_integer :=5) is
v_num_fam       pls_integer :=p_families;
v_sn_sd         pls_integer;
v_fn1_sd        pls_integer;
v_fn2_sd        pls_integer;
v_sn_c          pls_integer;
v_fn_m_l        pls_integer; -- male forenames low
v_fn_m_h        pls_integer; -- male forenames hi
v_fn_f_l        pls_integer; -- female forenames low
v_fn_f_h        pls_integer; -- female forenames hi
v_addr_sd       pls_integer;
v_addr_low      pls_integer;
v_addr_hi       pls_integer;
v_addr_id       pls_integer;
v_dob           date;
v_sn            varchar2(30);
v_fn1           varchar2(30);
v_fn2           varchar2(30);
v_sx            varchar2(1);
v_sn_keep_pct   pls_integer;
v_rename_pct    pls_integer;
v_pers_id       person.pers_id%type;
v_pena_id       person_name.pena_id%type;
v_end           pls_integer;
procedure get_name (p_sx  in varchar2
                   ,p_sn  in out varchar2
                   ,p_fn1 out varchar2
                   ,p_fn2 out varchar2 )
                     is
v_sn_sd    pls_integer;
v_fn1_sd   pls_integer;
v_fn2_sd   pls_integer;
v_twonames pls_integer;
begin
if p_sx ='M' then
  v_fn1_sd    := trunc(dbms_random.value(v_fn_m_l,v_fn_m_h+1));
  v_fn2_sd    := trunc(dbms_random.value(v_fn_m_l,v_fn_m_h+1));
  -- reduce (but not fully remove) duplicate forenames
  while v_fn1_sd = v_fn2_sd loop
    v_fn2_sd := trunc(dbms_random.value(v_fn_m_l,v_fn_m_h+1));
  end loop;
else
  v_fn1_sd    := trunc(dbms_random.value(v_fn_f_l,v_fn_f_h+1));
  v_fn2_sd    := trunc(dbms_random.value(v_fn_f_l,v_fn_f_h+1));
  while v_fn1_sd = v_fn2_sd loop
    v_fn2_sd := trunc(dbms_random.value(v_fn_f_l,v_fn_f_h+1));
  end loop;
end if;
v_twonames  := trunc(dbms_random.value*50)+1;
if p_sn is null then
  v_sn_sd     := trunc(dbms_random.value*v_sn_c)+1;
  select surname
  into p_sn
  from sn
  where surn_id = v_sn_sd;
end if;
select forename into p_fn1
from fn
where sex_ind= p_sx
and   forn_id = v_fn1_sd;
-- forename 2
if v_twonames >10 then
  select forename into p_fn2
  from fn
  where sex_ind= v_sx
  and   forn_id = v_fn2_sd;
else
  p_fn2 := '';
end if;
exception
when no_data_found then
  dbms_output.put_line ('errored on sn '||v_sn_sd||' fn1 '||v_fn1_sd||' fn2 '||v_fn2_sd);
  raise;
end;
--
procedure name_loop (p_pena_id      in number
                    ,p_pers_id      in number
                    ,p_sx           in varchar2
                    ,p_sn           in varchar2
                    ,p_prev_start   in date
                    ,p_sn_keep_pct  in number := 70
                    ,p_rename_pct   in number :=70) is
-- will loop 1 and 40% of time repeat, inserting a new name
-- main loop
v_end        number;
v_sn         varchar2(30);
v_fn1        varchar2(30);
v_fn2        varchar2(30);
v_pena_id    number    := p_pena_id;
v_prev_start date      := p_prev_start;
v_new_start  date;
v_prev_end   date      := trunc(sysdate);
v_rename_pct number    := p_rename_pct;
begin
  -- increase chance of one name, inc from 100 to 110
  v_end := trunc(dbms_random.value*110)+1;
  while v_end >v_rename_pct loop
    if trunc(dbms_random.value*100) > p_sn_keep_pct then
      v_sn := '';
    end if;
    get_name(p_sx,v_sn,v_fn1,v_fn2);
    v_new_start := v_prev_end -
                     trunc(dbms_random.value*(v_prev_end-v_prev_start)*.33);
    update person_name
    set    start_date = v_new_start
    where  pena_id = v_pena_id;
    insert into person_name
    (PENA_ID
    ,PERS_ID
    ,SURNAME
    ,FIRST_FORENAME
    ,SECOND_FORENAME
    ,PERS_TITLE
    ,START_DATE
    ,end_date
    ,pena_comment)
    values
    (pena_seq.nextval
    ,p_pers_id
    ,v_sn
    ,v_fn1
    ,v_fn2
    ,decode (v_sx,'M','MR'
                 ,'F','MRS'
                 ,     'DR')
    ,p_prev_start
    ,v_new_start-1
    -- BELOW IS TO PAD OUT TABLE A BIT
    ,'PERSON SURNAME IS NOW '||V_SN||' AND FIRST FORENAME IS '||V_FN1||' '
      ||LPAD ('A',255,'A') )
      returning pena_id into v_pena_id;
    v_prev_end := v_new_start-1;
    v_end :=trunc(dbms_random.value*100)+1;
  end loop;
end;
--
--
begin
  dbms_application_info.read_module(pv_init_module,pv_init_action);
  dbms_application_info.set_module(module_name => 'POP_PERS',action_name =>'START');
  pv_process_name       :='POP_PERS';
  pv_log_level          := p_log_level;
  pv_executed_timestamp := systimestamp;
  write_log ('started at '||to_char(pv_executed_timestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  -- get number of names in DB
  select count(*) into v_sn_c from sn;
  -- tables to small to worry about min/max FTS
  select min(forn_id),max(forn_id) into v_fn_m_l,v_fn_m_h from fn where sex_ind='M';
  select min(forn_id),max(forn_id) into v_fn_f_l,v_fn_f_h from fn where sex_ind='F';
  if pv_log_level >3 then
    write_log('surnames: '||v_sn_c||' male forenames: '||v_fn_m_l||'-'||v_fn_m_h
                                  ||' female forenames: '||v_fn_f_l||'-'||v_fn_f_h);
  end if;
  -- get address id range
  -- performs better done as two steps if the index exists, which it should
  select min(addr_id)
  into v_addr_low
  from address;
  select max(addr_id)
  into v_addr_hi
  from address;
  write_log ('addr low is '|| to_char(v_addr_low||'  high is '|| to_char(v_addr_hi)));
  -- number of famillies to create
  dbms_application_info.set_action ('CREATING FAMILIES');
  for a in 1..v_num_fam loop
    if mod(a,2) = 1 then
      v_sx          := 'M';
      v_rename_pct  := 75;
      v_sn_keep_pct := 60;
    else
      v_sx          := 'F';
      v_rename_pct  := 65;
      v_sn_keep_pct := 30;
    end if;
    get_name(v_sx
            ,v_sn
            ,v_fn1
            ,v_fn2);
    v_addr_id := trunc(dbms_random.value*(v_addr_hi-v_addr_low)+v_addr_low);
    v_dob := trunc(sysdate) - trunc(dbms_random.value*(20000)+10000);
    insert into person
    (PERS_ID
    ,SURNAME
    ,FIRST_FORENAME
    ,SECOND_FORENAME
    ,PERS_TITLE
    ,SEX_IND
    ,DOB
    ,ADDR_ID
    ,STAFF_IND
    ,LAST_CONTACT_ID
    ,PERS_COMMENT)
    values
    (pers_seq.nextval
    ,v_sn
    ,v_fn1
    ,v_fn2
    ,decode (v_sx,'M','MR'
                 ,'F','MRS'
                 ,     'DR')
    ,v_sx
    ,v_dob
    ,v_addr_id
    ,'N'
    ,null
    ,LPAD ('C',400,'C'))
    returning pers_id into v_pers_id ;
    insert into person_name
    (PENA_ID
    ,PERS_ID
    ,SURNAME
    ,FIRST_FORENAME
    ,SECOND_FORENAME
    ,PERS_TITLE
    ,START_DATE
    ,PENA_COMMENT)
    values
    (pena_seq.nextval
    ,v_pers_id
    ,v_sn
    ,v_fn1
    ,v_fn2
    ,decode (v_sx,'M','MR'
                 ,'F','MRS'
                 ,     'DR')
    ,v_dob
    ,'ORIGINAL SURNAME IS '||V_SN||' AND FIRST FORENAME IS '||V_FN1||' '
      ||LPAD ('B',255,'B') )
      returning pena_id into v_pena_id;
    name_loop (v_pena_id,v_pers_id,v_sx,v_sn,v_dob,v_sn_keep_pct,v_rename_pct);
    --
    --
    if trunc(dbms_random.value*10) > 3 then
    -- insert partner
      if v_sx = 'M' then
        v_sx          := 'F';
        v_rename_pct  := 45;
        v_sn_keep_pct := 30;
      else
        v_sx          := 'M';
        v_rename_pct  := 75;
        v_sn_keep_pct := 80;
      end if;
      --
      get_name(v_sx
              ,v_sn
              ,v_fn1
              ,v_fn2);
      -- below gets partner's age, about 7 years either way of orig
      v_dob := v_dob + trunc((dbms_random.value*5000)-2500);
      insert into person
      (PERS_ID
      ,SURNAME
      ,FIRST_FORENAME
      ,SECOND_FORENAME
      ,PERS_TITLE
      ,SEX_IND
      ,DOB
      ,ADDR_ID
      ,STAFF_IND
      ,PERS_COMMENT)
      values
      (pers_seq.nextval
      ,v_sn
      ,v_fn1
      ,v_fn2
      ,decode (v_sx,'M','MR'
                   ,'F','MRS'
                   ,     'DR')
      ,v_sx
      ,v_dob
      ,v_addr_id
      ,'N'
      ,LPAD ('S',400,'S'))
      returning pers_id into v_pers_id;
      insert into person_name
      (PENA_ID
      ,PERS_ID
      ,SURNAME
      ,FIRST_FORENAME
      ,SECOND_FORENAME
      ,PERS_TITLE
      ,START_DATE
      ,PENA_COMMENT)
      values
      (pena_seq.nextval
      ,v_pers_id
      ,v_sn
      ,v_fn1
      ,v_fn2
      ,decode (v_sx,'M','MR'
                   ,'F','MRS'
                   ,     'DR')
      ,v_dob
      ,'ORIGINAL SURNAME IS '||V_SN||' AND FIRST FORENAME IS '||V_FN1||' '
      ||LPAD ('S',255,'S') )
      returning pena_id into v_pena_id;
      name_loop (v_pena_id,v_pers_id,v_sx,v_sn,v_dob,v_sn_keep_pct,v_rename_pct);
    end if; -- partner
    --now kids
    -- weighted by age of parents
    if trunc(dbms_random.value*100) < 30 + (trunc(sysdate-v_dob)/500) then
      v_end := 70;
      while v_end > 60 loop
        if trunc((dbms_random.value*2)) = 1 then
          v_sx := 'M';
        else
          v_sx := 'F';
        end if;
        v_sn_keep_pct := 50;
        v_rename_pct := 85;
        v_dob := trunc(sysdate) - trunc((dbms_random.value*5000)+10);
      --
        get_name(v_sx
                ,v_sn
                ,v_fn1
                ,v_fn2);
        insert into person
        (PERS_ID
        ,SURNAME
        ,FIRST_FORENAME
        ,SECOND_FORENAME
        ,PERS_TITLE
        ,SEX_IND
        ,DOB
        ,ADDR_ID
        ,STAFF_IND
        ,PERS_COMMENT)
        values
        (pers_seq.nextval
        ,v_sn
        ,v_fn1
        ,v_fn2
        ,decode (v_sx,'M','MASTER'
                     ,'F','MISS'
                   ,     'GITTY')
        ,v_sx
        ,v_dob
        ,v_addr_id
        ,'N'
        ,LPAD ('K',400,'K'))
        returning pers_id into v_pers_id;
      insert into person_name
        (PENA_ID
        ,PERS_ID
        ,SURNAME
        ,FIRST_FORENAME
        ,SECOND_FORENAME
        ,PERS_TITLE
        ,START_DATE
        ,PENA_COMMENT)
        values
        (pena_seq.nextval
        ,pers_seq.currval
        ,v_sn
        ,v_fn1
        ,v_fn2
        ,decode (v_sx,'M','MASTER'
                     ,'F','MISS'
                     ,     'GITTY')
        ,v_dob
        ,'ORIGINAL SURNAME IS '||V_SN||' AND FIRST FORENAME IS '||V_FN1||' '
        ||LPAD ('K',255,'K') )
        returning pena_id into v_pena_id;
        name_loop (v_pena_id,v_pers_id,v_sx,v_sn,v_dob
                ,v_sn_keep_pct,v_rename_pct);
        v_end := trunc (dbms_random.value*100);
      end loop;
    end if;
  v_sn :='';
  if mod(a,5000) = 0 then
    commit;
    write_log ('commited on '||to_char(a)
             ||'  at '||to_char(sysdate,'DDMMYY HH24:MI:SS'));
  end if;
  end loop;
  commit;
  write_log ('ended at '||to_char(systimestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  write_log ('elapsed is '||substr(systimestamp-pv_executed_timestamp,10,20));
  dbms_application_info.set_module(pv_init_module,pv_init_action);
exception
  when others then
    pv_err_msg:=dbms_utility.format_error_stack;
    pv_err_stack := dbms_utility.format_error_backtrace;
    write_log(pv_err_msg,'E',SQLCODE);
    write_log(pv_err_stack,'E',SQLCODE);
    dbms_application_info.set_module(pv_init_module,pv_init_action);
    raise;
end;
--
procedure pop_cuor_col(p_days    in pls_integer :=10
                      ,p_start   in date :=trunc(sysdate-10)
                      ,p_log_level in pls_integer :=3
                      ,p_recs_per_day in pls_integer :=1000)is
type array_cuor is table of customer_order%rowtype index by binary_integer;
type array_col  is table of customer_order_line%rowtype  index by binary_integer;
cuor_array array_cuor;
col_array  array_col;
v_count    pls_integer;
v_count2   pls_integer;
v_date     date;
v_no   pls_integer :=0;
v_addr_id_min pls_integer;
v_addr_id_max pls_integer;
v_pers_id_min pls_integer;
v_pers_id_max pls_integer;
begin
  pv_process_name       :='POP_CUOR_COL';
  pv_log_level          := p_log_level;
  pv_executed_timestamp := systimestamp;  -- for each day, create data 
  write_log ('started');  
  select max(pers_id),min(pers_id) into v_pers_id_max,v_pers_id_min from person;
  select max(addr_id),min(addr_id) into v_addr_id_max,v_addr_id_min from person;
  write_log ('initiated');    
  <<day_loop>>
  for i in 1..p_days loop
    write_log('day '||to_char(i));
    v_date :=p_start+(i-1)+(dbms_random.value/(p_recs_per_day/2));
    write_log('seed daytime is '||to_char(v_date,'yymmdd hh24:mi:ss'));
    v_count :=0; --for customer_order
    v_count2:=0; --for customer_order_line
    <<cuor_loop>>
    while v_date < p_start+i loop
      v_count:=v_count+1;
      -- do the data creation
      v_no := v_no+1;
      cuor_array(v_count).id           := v_no;
      cuor_array(v_count).created_dt   := v_date;
      cuor_array(v_count).customer_id  := trunc(dbms_random.value(v_pers_id_min,v_pers_id_max));
      cuor_array(v_count).address_id   := trunc(dbms_random.value(v_pers_id_min,v_pers_id_max));
      cuor_array(v_count).status       := 'C';
      cuor_array(v_count).completed_dt := v_date + dbms_random.value(0,2);
      <<col_loop>>
      for j in 1..trunc(dbms_random.value(1,11)) loop
        v_count2 :=v_count2+1;
        col_array(v_count2).cuor_id     := v_no;
        col_array(v_count2).created_dt  := v_date;
        col_array(v_count2).line_no   := j;
        col_array(v_count2).product_code  :=trunc(dbms_random.value(1,10000));
        col_array(v_count2).unit_number   :=trunc(dbms_random.value(1,11));
        col_array(v_count2).unit_cost   :=trunc(dbms_random.value(1,100),2);
      end loop col_loop;
      exit when v_count>p_recs_per_day;
      v_date :=v_date+(dbms_random.value/(p_recs_per_day/2));
    end loop cuor_loop;
    write_log(' count is '||to_char(v_count)||' count2 is '||to_char(v_count2)
              ||'  and vdate is '||to_char(v_date,'dd hh24:mi:ss'));
    write_log ('number of orders is '||to_char(cuor_array.count)||' number of lines is '||to_char(col_array.count)
               ||' and created '||to_char(v_no)||' in total');
    --
    forall idx in indices of cuor_array
    insert into customer_order
    values cuor_array(idx);
    forall idx2 in indices of col_array
    insert into customer_order_line
    values col_array(idx2);
    --
    cuor_array.delete; -- so we carry no records over by accident
    col_array.delete;
    commit;
   end loop day_loop; 
  null;
end;
--
procedure summarize_orders is
-- will be called to process whatever orders are outstanding?
p_start_dt date;
p_end_dt   date;
p_expected_rows pls_integer;
v_count         pls_integer;
v_loop_count    pls_integer;
v_last          pls_integer;
v_abandon       process_master.abandon_fl%type;
cursor get_cos is
  select cuor.id, cuor.created_dt,cuor.customer_id
      ,sum(col.unit_number) num_items
      ,sum(col.unit_number*col.unit_cost) tot_value
  from customer_order cuor
      ,customer_order_line col
  where cuor.id =col.cuor_id
  and   cuor.created_dt = col.created_dt
  and   cuor.created_dt >   p_start_dt 
  and   cuor.created_dt <=  p_end_dt
  group by cuor.id, cuor.created_dt,cuor.customer_id
  order by cuor.created_dt;
type array_get_cos is table of get_cos%rowtype index by pls_integer;
get_cos_array  array_get_cos;
abort_run   exception;
--
begin
  dbms_application_info.read_module(pv_init_module,pv_init_action); 
  pv_process_name       :='SUMMARIZE_ORDERS';
  pv_log_level          :=5;
  dbms_application_info.set_module(module_name => pv_process_name,action_name =>'START');
  pv_executed_timestamp := systimestamp;
  pv_prma_rec:=null;  pv_prru_rec:=null;
  write_log('collecting and updating the process_master for '||pv_process_name);
  get_prma(pv_process_name,false); -- Need to see if last run errorored or is running
  if pv_prma_rec.status = 'I' then -- want to simply stop Altering PRMA or PRRU would
                                   -- mess up running version
     write_log('attempted to run whilst already running, so aborted');
     raise abort_run;
  elsif pv_prma_rec.status in ('E','R') then
    null; -- we leave as error, record the run, skip processing and close as error.
  else pv_prma_rec.status :='I';
  end if;
  pv_prma_rec.last_executed_timestamp :=pv_executed_timestamp;
  upd_prma(pv_prma_rec,'SU');
  pv_log_level               := pv_prma_rec.log_level;
  pv_prru_rec.process_name       := pv_prma_rec.process_name;
  pv_prru_rec.start_timestamp    := pv_prma_rec.last_executed_timestamp;
  pv_prru_rec.status             := pv_prma_rec.status;
  pv_prru_rec.log_level          := pv_prma_rec.log_level;
  pv_prru_rec.process_range      := pv_prma_rec.process_range;
  pv_prru_rec.batch_size         := pv_prma_rec.batch_size;
  pv_prru_rec.max_window         := pv_prma_rec.max_window;
  pv_prru_rec.process_delay      := pv_prma_rec.process_delay;
  pv_prru_rec.start_timestamp1   := pv_prma_rec.last_timestamp1;
  pv_prru_rec.records_processed  := 0;
  pv_prru_rec.records_skipped    := 0;
  pv_prru_rec.records_errored    := 0;
  write_log('creating the process_run entry and updating');
  ins_prru(pv_prru_rec);
  write_plog ('started at '||to_char(pv_executed_timestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  if pv_prma_rec.abandon_fl = 'Y' then
    write_plog('process set to Abandon so halted');
  elsif pv_prma_rec.status in ('E') then
    write_plog('process in error - halting');
  else
    -- This is a key step. Get the range of values to process, 
    -- If this is a recovery run we know where we got up to for the in-flight step (only one in this case)
    -- via prma.last_prcessed 
    if pv_prma_rec.status ='R' then
      p_end_dt :=pv_prma_rec.window_end_timestamp1;
      p_expected_rows :=-1;
    else
      write_plog('identifying the range of data to process, limited by '||pv_prma_rec.process_range||' rows'
                 ||' and '||pv_prma_rec.max_window||' hours');
      with source_t as
         (select /*+ materialize */ created_dt
                                  ,rownum r1
          from (select created_dt
                from   customer_order
                where created_dt> pv_prma_rec.last_timestamp1
                and   created_dt <= pv_prma_rec.last_timestamp1 +(interval '1' hour*pv_prma_rec.max_window)
                and created_dt   <= pv_prma_rec.last_executed_timestamp -(interval '1' minute*pv_prma_rec.process_delay)
                order by created_dt)
          where rownum <=pv_prma_rec.process_range
          )
      select max(created_dt)
             ,max(r1)
      into p_end_dt
          ,p_expected_rows
      from source_t st;
    end if;
--    where st.r1 = (select max(r1) from source_t);
    write_plog ('would process up to '||to_char(p_end_dt,'DD-MON-YY HH24:MI:SS')||' processing '||to_char(p_expected_rows));
    pv_prma_rec.window_start_timestamp1 := pv_prma_rec.last_timestamp1;
    pv_prma_rec.window_end_timestamp1   := p_end_dt;
    pv_prma_rec.stage                       :=1;
    upd_prma(pv_prma_rec,'WU');
    pv_prru_rec.start_timestamp1 := pv_prma_rec.last_timestamp1;
    pv_prru_rec.end_timestamp1   := p_end_dt;
    upd_prru(pv_prru_rec,'WU');
    -- We are now ready to proces the data.
    p_start_dt :=pv_prru_rec.start_timestamp1;
    if pv_log_level >=5 then
      write_plog(' processing range '||to_char(p_start_dt,'DD-MON-YY HH24:MI:SS')||' to '
                                     ||to_char(p_end_dt,'DD-MON-YY HH24:MI:SS'));
    end if;
    v_count :=0;
    v_loop_count :=0;
    open get_cos;
    <<main_loop>>
    loop
      fetch get_cos bulk collect
      into get_cos_array
      limit pv_prma_rec.batch_size;
      exit when get_cos_array.count = 0;
      v_loop_count:=v_loop_count+1;
      v_last      := get_cos_array.count;
      v_count :=v_count+v_Last;
      if pv_prma_rec.log_level >=5 then
        write_plog('iteration '||v_loop_count||' processing '||v_last);
      end if;
      for i in 1..v_last loop
         if get_cos_array(i).num_items >60 then 
            get_cos_array(i).tot_value :=round(get_cos_array(i).tot_value*.9,2);
         end if;
      end loop;
      -- push date into the final table
      forall i in indices of get_cos_array
      insert into customer_order_summary
--        (id,created_dt,customer_id,num_Items,tot_value)
      values
        get_cos_array(i);
--      (get_cos_array(i).id
--      ,get_cos_array(i).created_dt
--      ,get_cos_array(i).customer_id
--      ,get_cos_array(i).num_items
--      ,get_cos_array(i).tot_value);
      commit;
      -- now update where we are
      pv_prma_rec.last_timestamp1 :=get_cos_array(v_last).created_dt;
      upd_prma(pv_prma_rec,'CF');
      pv_prru_rec.records_processed :=v_count;
      upd_prru(pv_prru_rec,'CS');
      dbms_lock.sleep(10);
      -- check for abandon
      select abandon_fl into pv_prma_rec.abandon_fl
      from  process_master where process_name=pv_prma_rec.process_name;
      exit when pv_prma_rec.abandon_fl='Y';
    end loop main_loop;
--  while v_count < p_rows loop
--    for a in 1..v_chunk_size loop
  --
  end if; -- abandon_check
  if pv_prma_rec.abandon_fl='Y' then
    pv_prru_rec.status     :='A';   pv_prma_rec.status     :='A';
  elsif pv_prma_rec.status = 'E' then
    null;
  else
    pv_prru_rec.status     :='C';   pv_prma_rec.status     :='C';
  end if;  
  pv_prru_rec.completed_timestamp  :=systimestamp;
  upd_prru(pv_prru_rec,'CF');
  pv_prma_rec.stage  :=null;
  upd_prma(pv_prma_rec,'CF');
  write_plog ('ended at '||to_char(systimestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  dbms_application_info.set_module(pv_init_module,pv_init_action);
exception
  when abort_run then 
    null;  -- ie just end processing, having changed nothing
  when others then
    pv_err_msg:=dbms_utility.format_error_stack;
    pv_err_stack := dbms_utility.format_error_backtrace;
    rollback;
    write_plog(pv_err_msg,'E',SQLCODE);
    write_plog(pv_err_stack,'E',SQLCODE);
    write_error(pv_err_msg,sqlcode,'F');
    -- update the control tables to show it errored
    pv_prru_rec.status               :='E';
    pv_prru_rec.completed_timestamp  :=systimestamp;
    upd_prru(pv_prru_rec,'CF');
    pv_prma_rec.status :='E';
    pv_prma_rec.stage  :=null;
    pv_prma_rec.abandon_fl :='N';
    upd_prma(pv_prma_rec,'CF');
    dbms_application_info.set_module(pv_init_module,pv_init_action);
    raise;    
end summarize_orders;
--
procedure do_ddl(p_obj      in varchar2
                ,p_activity in varchar2 :='T') is
v_statement   varchar2(4000);
begin
  -- should I get the action name, save it and re-set it at end?
  if p_activity='T' then
     v_statement :='truncate table '||p_obj;
  end if;
  write_log(' about to '||v_statement);
  execute immediate v_statement;
exception
  when others then
    if sqlcode=-942 then
      write_log('object '||p_obj||' did not exist to undergo '||p_activity);
    else
      raise;
   end if;
end do_ddl;
--
procedure trunc_tabs(p_log_also in varchar2 :='N') is
-- Note sequences are not reset (and should not matter)
-- Writes to PLSQL_LOG
begin
  dbms_application_info.set_module(module_name => 'TRUNC_TABS',action_name =>'START');
  pv_process_name       :='POP_SOURCE';
  pv_log_level          := 5;
  pv_executed_timestamp := systimestamp;
  write_log ('started at '||to_char(pv_executed_timestamp,'YY-MM-DD HH24:MI:SS.FF3'));
  if upper(p_log_also) = 'Y' then
    do_ddl('PLSQL_LOG','T');
  end if;
  do_ddl('SN','T');
  do_ddl('FN','T');
  do_ddl('ROAD_NAME','T');
  do_ddl('ROAD_TYPE','T');
  do_ddl('TOWN_NAME','T');
  do_ddl('PERSON_NAME','T');
  do_ddl('PERSON','T');
  do_ddl('ADDRESS','T');
  write_log(' ending trunc_tabs');
end trunc_tabs;
--
begin
  null;
end pna_maint;
/