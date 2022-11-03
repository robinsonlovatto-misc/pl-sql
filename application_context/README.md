**Application Context**

Application context is very usefull to define session variables.
One widely use case is for define the web application user, in most cases is used only one database user for rhe web application and many web users. So, when the web user is connected to Oracle through the connection pool, the context must be changed to the web user connected.
