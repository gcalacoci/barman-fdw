-- Script for setting up the Barman FDW
--------------------------------------------

CREATE EXTENSION multicorn;

CREATE SERVER barman_e_fdw FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'barman_fdw.BarmanFDW.BarmanForeignDataWrapper',
  barman_host '127.0.0.1',
  barman_user 'barman'
);

CREATE SCHEMA barman_schema;

IMPORT FOREIGN SCHEMA barman FROM SERVER barman_e_fdw INTO barman_schema;
