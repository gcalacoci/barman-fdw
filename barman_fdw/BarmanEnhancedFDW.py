# Copyright (C) 2016 Giulio Calacoci
#
# This file is part of BarmanFDW.
#
# BarmanFDW is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# BarmanFDW is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with BarmanFDW.  If not, see <http://www.gnu.org/licenses/>.

import json
import subprocess
from logging import DEBUG, ERROR

from multicorn import ColumnDefinition, ForeignDataWrapper, TableDefinition
from multicorn.utils import log_to_postgres


class BarmanEnhancedForeignDataWrapper(ForeignDataWrapper):
    def __init__(self, options, columns):
        """
        Init method for the Foreign Data Wrapper.

        Used to manage the options necessary to run barman

        :type options: Options passed during the creation of the FDW
        :type columns: the columns of the foreign table
        """
        super(BarmanEnhancedForeignDataWrapper, self).__init__(options,
                                                               columns)

        if 'table_name' not in options:
            log_to_postgres('The table_name parameter is required', ERROR)
        if 'barman_user' not in options:
            log_to_postgres('The barman_user parameter is required', ERROR)
        if 'barman_host' not in options:
            log_to_postgres('Option barman_host is required for '
                            'the Barman FDW setup.', ERROR)

        self.schema = options['schema'] if 'schema' in options else None
        self.table_name = options['table_name']
        self.barman_host = options['barman_host']
        self.barman_user = options['barman_user']

        self._row_id_column = 'server'

        # The columns we'll be using (defaults to 'all'):
        self.columns = columns

        log_to_postgres('Barman FDW Config options:  %s' % options, DEBUG)
        log_to_postgres('Barman FDW Config columns:  %s' % columns, DEBUG)

    def execute(self, quals, columns, **kwargs):
        """
        This method is called every time a SELECT is executed
        on the foreign table.
        Manage the SELECT statements using the table_name option
        to build the different rows.

        :param list quals:a list of conditions which are used
          are used in the WHERE part of the select and can be used
          to restrict the number of the results
        :param list columns: the columns of the foreign table
        """
        # Execute the diagnose through ssh
        errors, result = self._execute_barman_cmd("barman diagnose")
        if errors:
            # if any error occurred, return
            return
        # Otherwise, load the results using json
        result = json.loads(result)
        servers = result['servers']
        for server, values in servers.items():
            if self.table_name == 'server_config':
                # if the query have been executed on the server_config table,
                # prepare the results
                line = {
                    'server': server,
                    'description': values['config']['description'],
                    'config': json.dumps(values['config'])
                }
                yield line
            elif self.table_name == 'server_status':
                # if the query have been executed on the server_status table,
                # prepare the results iterating through the fields of the
                # status json object.
                line = {'server': server}
                for key, value in values['status'].items():
                    line[key] = value
                yield line
            else:
                # Otherwise the query have been executed on one of the
                # the tables representing a barman server.
                # prepare the results iterating through the fields of the
                # status json object.
                server = server.replace('-', '_').replace('.', '_')
                for backup, properties in values['backups'].items():
                    # Iterates the list of servers looking for the right one.
                    if server == self.table_name.replace('-',
                                                         '_').replace('.',
                                                                      '_'):
                        # Create the result line.
                        line = {}
                        for key in properties.keys():
                            line[key] = properties[key]
                        yield line

    @classmethod
    def import_schema(self, schema, srv_options, options, restriction_type,
                      restricts):
        """
        Called on an IMPORT FOREIGN SCHEMA command.

        Populates a PostgreSQL schema using the json output of the
        barman diagnose command.
        This method iterates through the results of the diagnose command,
        creates a foreign table for every server using the field of the backup
        json object as columns of the table.
        Additionally creates 2 tables: 'server_config' and 'server_status'
        'server_config' contains the name, description and json
        configuration of every barman server stored in a jsonb column.
        'server_status' contains the status of every server,
        using as columns the field of the status json object.

        Every table is created using the 'table_name' server option.
        The table_name is used during the execution of SELECT statements to
        retrieve the correct table for the execution of the query.
        """
        # Executes the barman diagnose command through ssh connection
        errors, result = self._execute_barman_cmd("barman diagnose",
                                                  srv_options['barman_user'],
                                                  srv_options['barman_host'])
        # if an error is present, return.
        if errors:
            return
        # Load the result using json
        result = json.loads(result)
        servers = result['servers']
        tables = []
        # Iterates through the available servers
        for server, values in servers.items():
            # Encodes the name of the server replacing - and . characters
            # with _
            server = server.replace('-', '_').replace('.', '_')
            log_to_postgres('schema %s table %s' % (schema, server), DEBUG)
            for backup, properties in values['backups'].items():
                # Creates a table for every server. uses the fields of the
                # keys of the json object as columns.
                table = TableDefinition(table_name=server)
                table.options['schema'] = schema
                # set the encoded server name as table name
                table.options['table_name'] = server
                self._format_table(properties, table)
                # Add the table to the list of the tables.
                tables.append(table)
        # Create the server_config table
        table_config = TableDefinition(table_name='server_config')
        table_config.options['schema'] = schema
        # Set the table name
        table_config.options['table_name'] = 'server_config'
        # Create the columns
        table_config.columns.append(
            ColumnDefinition(
                column_name='server',
                type_name='character varying'
            ))
        table_config.columns.append(
            ColumnDefinition(
                column_name='description',
                type_name='character varying'
            ))
        table_config.columns.append(
            ColumnDefinition(
                column_name='config',
                type_name='jsonb'
            ))
        # Add the table to the list of the tables.
        tables.append(table_config)
        # Create the server_status table
        table_status = TableDefinition(table_name='server_status')
        table_status.options['schema'] = schema
        table_status.options['table_name'] = 'server_status'
        # Iterates through the fields of the status json object,
        # and use them to create the columns of the table
        for server, values in servers.items():
            self._format_table(values['status'], table_status)
            break
        # Adds a column containing the server name.
        table_status.columns.append(
            ColumnDefinition(
                column_name='server',
                type_name='character varying'
            ))
        # Add the table to the list of the tables.
        tables.append(table_status)
        return tables

    @staticmethod
    def _format_table(properties, table):
        """
        Utility method. Given a dict and a TableDefinition object
        iterates through the dict and populate the table
        using the dict keys

        :param dict properties: the dict holding the keys
          used to build the table
        :param TableDefinition table: the table to build
        """
        for key in properties.keys():
            table.columns.append(
                ColumnDefinition(column_name=key,
                                 type_name='character varying'
                                 ))

    @staticmethod
    def _execute_barman_cmd(barman_cmd, barman_user, barman_host):
        """
        Utility method. Executes a barman command using ssh.
        Logs errors if something is written in the stderr

        :param str barman_cmd: the command we want to execute
        :param str barman_user: the ssh user
        :param str barman_host: the host to connect to
        """
        ssh = "%s@%s" % (barman_user, barman_host)
        cmd = subprocess.Popen(["ssh", "%s" % ssh, barman_cmd],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        output = cmd.communicate()
        errors = True if output[1] else False
        if errors:
            std_err = cmd.stderr.readlines()
            log_to_postgres("ERROR: %s" % std_err, ERROR)
        return errors, output[0]

    @property
    def rowid_column(self):
        return self._row_id_column

    def insert(self, new_values):
        """
        This method is invoked every time a SELECT is executed
        on the foreign table.
        """
        log_to_postgres('Barman FDW INSERT output:  %s' % new_values, DEBUG)
        # Build the backup command
        backup_cmd = "barman backup %s" % new_values['server_name']
        errors, result = self._execute_barman_cmd(backup_cmd,
                                                  self.barman_user,
                                                  self.barman_host)
        if errors:
            # Return if error are present
            return
        # We need to return something for the RETURNING
        # clause of the INSERT statement.
        return new_values
