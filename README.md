# barman-multicorn-postgresql-fdw
## Multicorn based PostgreSQL Foreign Data Wrapper for barman

This is a simple FDW that executes the `barman diagnose` command through ssh.
The FDW manage the json result of the barman command, and displays a summary
on a foreign table.


## How To Use The FDW:

* You need a barman installed on a unix server, SSH must be configured
such that there is no password prompt presented when connecting.
* Next you'll need Multicorn installed on your PostgreSQL.
*  You'll need to download THIS repository
  * <not yet present>
* And install its code into your database server as well:
  * `python ./setup.py install`
* Create the multicorn extension:
  * `create extension multicorn;`
* Create a **server** and a **foreign table**.
  * `create server barman_fdw foreign data wrapper multicorn options (
    wrapper 'barman_fdw.BarmanFDW.BarmanForeignDataWrapper',
    barman_host '127.0.0.1');`
  * `CREATE FOREIGN TABLE barman (
    server character varying,
    backups character varying,
    description character varying,
    config jsonb
) server barman_fdw;`
* Perform SELECT
  * `select * from barman;`

enjoy the results ;)
