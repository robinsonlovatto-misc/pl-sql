DECLARE
   l_start   PLS_INTEGER;
BEGIN
   l_start := DBMS_UTILITY.get_time;

   dbms_session.sleep(1.3);

   DBMS_OUTPUT.put_line (   
      'Elapsed time = '
      || TO_CHAR (DBMS_UTILITY.get_time - l_start) ||' ms.'
      );
END;
/
