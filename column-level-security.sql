/*HR needs to reference the column empno of the emp table.
Instead of grant the entire table as in 
grant references on emp to hr; 
Is possible to grant the permission only to the needed column as below.

The same is possible with UPDATE and INSERT.
*/

grant references (empno) on emp to hr;
