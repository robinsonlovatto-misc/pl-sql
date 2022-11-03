/*
As a general tip to avoid sql injection it is recommended to use bind variables and use dynamic SQL only if is really necessary.
When the dynamic sql is the solution, you can sanitize the inputs before concatenate with your statement using dbms_assert.enquote_literal
*/

--the WRONG way
CREATE OR REPLACE PROCEDURE show_sql_vulnerable (p_dummy VARCHAR2) IS
   l_stmt  varchar2(4000);
   l_dummy NUMBER;
   l_temp  varchar2(4000);
begin
   l_stmt := 'select count(1) from dual where dummy = '''|| p_dummy ||'''';
   dbms_output.put_line('l_stmt (vulnerable to sql injection)='||l_stmt);

   execute immediate l_stmt into l_dummy;
   IF l_dummy > 0 THEN
      dbms_output.put_line('The value of dummy (X) was correctly passed.');
   ELSE
     dbms_output.put_line('The dummy value is wrong.');
   END IF;
end;
/

--testing Result: The value of dummy (X) was correctly passed.
BEGIN
  show_sql_vulnerable('X');
END;
/

--testing Result: The dummy value is wrong.
BEGIN
  show_sql_vulnerable('WrongDummy');
END;
/

--testing Result: The value of dummy (X) was correctly passed. WRONG. SQL injection.
BEGIN
  show_sql_vulnerable('WrongDummy'' or ''1''=''1');
END;
/

--the RIGHT way
CREATE OR REPLACE PROCEDURE show_sql (p_dummy VARCHAR2) IS
   l_stmt  varchar2(4000);
   l_dummy NUMBER;
   l_temp  varchar2(4000);
begin
   declare
     l_possible_injection_exception  exception;
     pragma exception_init (l_possible_injection_exception,-6502);
   begin
     l_temp := sys.dbms_assert.enquote_literal(p_dummy);
   exception
     when l_possible_injection_exception then
       raise_application_error (-20001,'Possible SQL Injection Attack');
     when OTHERS then
       raise;
   end;
   
   l_stmt := 'select count(1) from dual where dummy = '||l_temp;
           
   dbms_output.put_line('l_stmt='||l_stmt);
   execute immediate l_stmt into l_dummy;
   
   IF l_dummy > 0 THEN
      dbms_output.put_line('The value of dummy (X) was correctly passed.');
   ELSE
     dbms_output.put_line('The dummy value is wrong.');
   END IF;
end;
/

--testing Result: The value of dummy (X) was correctly passed.
BEGIN
  show_sql('X');
END;
/
--testing Result: The dummy value is wrong.
BEGIN
  show_sql('WrongDummy');
END;
/
--testing Result: Error: Possible SQL inection attack
BEGIN
  show_sql('WrongDummy'' or ''1''=''1');
END;
/
   
