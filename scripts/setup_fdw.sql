-- Script for setting up the Barman FDW
--------------------------------------------

CREATE EXTENSION multicorn;

CREATE SERVER barman_fdw FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'barman_fdw.BarmanFDW.BarmanForeignDataWrapper',
  barman_host '127.0.0.1');

CREATE FOREIGN TABLE barman (
  server CHARACTER VARYING,
  backups CHARACTER VARYING,
  description CHARACTER VARYING,
  config  JSONB
) SERVER barman_fdw;
