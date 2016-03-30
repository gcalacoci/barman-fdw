-- Script for setting up the Barman FDW
--------------------------------------------

create extension multicorn;

create server barman_fdw foreign data wrapper multicorn options (
    wrapper 'barman_fdw.BarmanFDW.BarmanForeignDataWrapper',
    barman_host '127.0.0.1');

CREATE FOREIGN TABLE barman (
    server character varying,
    backups character varying,
    description character varying,
    config jsonb
) server barman_fdw;
