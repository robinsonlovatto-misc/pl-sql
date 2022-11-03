**Application Context**

Application context is very usefull to define session variables.
One widely use case is for define the web application user, in most cases is used only one database user for the web application and many web users. 

So, when the web user is connected to Oracle through the connection pool, the context must be changed to the web user connected.

Oracle provides a built-in namespace called *USERENV*, which describes the current session. The predefined parameters of namespace USERENV are listed below:

SYS_CONTEXT ('USERENV', 'IP_ADDRESS') e.g.
```
ACTION
AUDITED_CURSORID
AUTHENTICATED_IDENTITY
AUTHENTICATION_DATA
AUTHENTICATION_METHOD
BG_JOB_ID
CLIENT_INFO
CURRENT_BIND
CURRENT_SCHEMA
CURRENT_SCHEMAID
CURRENT_SQL
CURRENT_SQLn
CURRENT_SQL_LENGTH
DB_DOMAIN
DB_NAME
DB_UNIQUE_NAME
ENTRYID
ENTERPRISE_IDENTITY
FG_JOB_ID
GLOBAL_CONTEXT_MEMORY
GLOBAL_UID
HOST
IDENTIFICATION_TYPE
INSTANCE
INSTANCE_NAME
IP_ADDRESS
ISDBA
LANG
LANGUAGE
MODULE
NETWORK_PROTOCOL
NLS_CALENDAR
NLS_CURRENCY
NLS_DATE_FORMAT
NLS_DATE_LANGUAGE
NLS_SORT
NLS_TERRITORY
OS_USER
POLICY_INVOKER
PROXY_ENTERPRISE_IDENTITY
PROXY_GLOBAL_UID
PROXY_USER
PROXY_USERID
SERVER_HOST
SERVICE_NAME
SESSION_USER
SESSION_USERID
SESSIONID
SID
STATEMENTID
TERMINAL
```
