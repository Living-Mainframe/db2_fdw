\set ECHO all
CREATE DATABASE regtest;
GRANT ALL PRIVILEGES ON DATABASE regtest to postgres;
\c regtest
-- Install extension
CREATE EXTENSION IF NOT EXISTS db2_fdw;
-- Install FDW Server
CREATE SERVER IF NOT EXISTS sample FOREIGN DATA WRAPPER db2_fdw OPTIONS (dbserver 'SAMPLE');
-- Map a user
CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user 'db2inst1', password 'db2inst1');
-- CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user '', password '');
-- Prepare a local schema
CREATE SCHEMA IF NOT EXISTS sample;
-- Import the complete sample db into the local schema
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO sample;
-- list imported tables
\detr+ sample.*
-- drop an imported table
DROP FOREIGN TABLE IF EXISTS sample.org;
-- recreate it manually
CREATE FOREIGN TABLE sample.org (
                  DEPTNUMB SMALLINT OPTIONS (key 'yes') NOT NULL ,
                  DEPTNAME VARCHAR(14) ,
                  MANAGER SMALLINT ,
                  DIVISION VARCHAR(10) ,
                  LOCATION VARCHAR(13)
                   )
      SERVER sample OPTIONS (schema 'DB2INST1',table 'ORG');
-- remove its content
delete from sample.org;
-- repopulate the content
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(10,'Head Office',160,'Corporate','New York');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(15,'New England',50,'Eastern','Boston');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(20,'Mid Atlantic',10,'Eastern','Washington');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(38,'South Atlantic',30,'Eastern','Atlanta');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(42,'Great Lakes',100,'Midwest','Chicago');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(51,'Plains',140,'Midwest','Dallas');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(66,'Pacific',270,'Western','San Francisco');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(84,'Mountain',290,'Western','Denver');
-- inquire the content
select * from sample.org;
select * from sample.sales;
-- test a simple join
select * from sample.employee a, sample.sales b where a.lastname = b.sales_person;
-- create a local table importing its structure and content from an fdw table
create table sample.orgcopy as select * from sample.org;
\d+ sample.org*
drop table sample.orgcopy;
-- cleanup
\c postgres
DROP DATABASE regtest;
--