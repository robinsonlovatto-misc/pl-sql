DECLARE
   l_start   PLS_INTEGER;
BEGIN
   l_start := DBMS_UTILITY.get_time;

   dbms_session.sleep(1);

   DBMS_OUTPUT.put_line (   
      'Elapsed time = '
      || TO_CHAR ((DBMS_UTILITY.get_time - l_start)*10) ||' ms. '||TO_CHAR ((DBMS_UTILITY.get_time - l_start)/100)||' s. ');
END;
/
