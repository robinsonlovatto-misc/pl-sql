**Control Framework for Automated Tasks**

This is a framework to control automated tasks (or any task actually) developed by the authors of the book [Real-World SQL and PL/SQL: Advice from the Experts](https://www.mhprofessional.com/real-world-sql-and-pl-sql-advice-from-the-experts-9781259640971-usa-group). As a contribution, I only made a small fix in the pna_maint.get_prma procedure.

The most simple example of an automated test can be seen in the pna_maint.test_auto procedure (running the code below):

```
begin
  -- Call the procedure
  pna_maint.test_auto;
end;
```

Tables that  are part of the framework:

PROCESS_MASTER: main table of the automated process, holds some last execution values.

PROCESS_RUN: detailed table, holds information about each run .

PROCESS_ERROR: holds error information about the process.

PROCESS_LOG: holds informational data about the process.

PLSQL_LOG: ordinary log table, that means it is not related to any process_* table.

---

Another aspect of the code I think is interesting is the way the utl_call_stack is used in the procedure pna_maint.gen_err.

---

I also want to mention the procedures to generate randomic data, very interesting way of generate thousands of records with a little source data. The procedures are:
pna_maint.pop_source, pna_maint.pop_addr_batch, pna_maint.pop_pers and pna_maint.pop_cuor_col.
