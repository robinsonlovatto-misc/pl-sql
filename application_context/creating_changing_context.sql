--Creating a context
CREATE CONTEXT web_ctx USING set_web_ctx;

--creating a procedure to add/change context values
CREATE OR REPLACE PROCEDURE set_web_ctx (
   p_attr IN VARCHAR2, p_val IN VARCHAR2
) IS
BEGIN
   DBMS_SESSION.set_context ('WEB_CTX', p_attr, p_val);
END;
/

--creating a procedure to change the web application user
CREATE OR REPLACE PROCEDURE set_user_web_ctx (
   p_webuser IN VARCHAR2
) IS
BEGIN
   DBMS_SESSION.set_context ('WEB_CTX', 'WEBUSER', p_webuser);
END;
/

--testing
BEGIN
  set_user_web_ctx ('MARTIN');
  DBMS_OUTPUT.put_line(sys_context('WEB_CTX','WEBUSER'));
END;
/
