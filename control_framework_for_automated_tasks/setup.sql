-- cre_ch9_demos
-- create all the demonstration packages and tables for Chapter 9 of Real World SQL & PL/SQL
--
set timi on
prompt creating the PNA_CRE package
@cre_pna_cre_ch9.sql
prompt and creating the tables
exec pna_cre.cre_tabs
prompt creating the PROTABS_CRE package
@cre_protabs_cre_ch9.sql
prompt and creating the tables
exec protabs_cre.cre_tabs
prompt creating the PNA_MAINT package
@cre_pna_maint_ch9.sql
prompt populating source tables, will take under a second
exec pna_maint.pop_source
prompt populating the ADDRESS table with 1 million records, will take a minute or so
exec pna_maint.pop_addr_batch
prompt populating the PERSON and PERSON_NAME tables, will take 1 to 3 minutes
exec pna_maint.pop_pers
prompt populating the CUSTOMER_ORDER and CUSTOMER_ORDER_LINES table. 1-2 mins
exec pna_maint.pop_cuor_col(60,trunc(sysdate)-60,3,10000)
prompt finished
