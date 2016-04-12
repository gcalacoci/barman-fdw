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
        This method is invoked every time a SELECT is executed
        on the foreign table.

        :param list quals:a list of conditions which are used
          are used in the WHERE part of the select and can be used
          to restrict the number of the results
        :param list columns: the columns of the foreign table
        """
        # create a client object using the apikey
        # Ports are handled in ~/.ssh/config since we use OpenSSH
        diagnose = "barman diagnose"
        barman_host = "%s@%s" % (self.barman_user,
                                 self.barman_host)
        ssh = subprocess.Popen(["ssh", "%s" % barman_host, diagnose],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        output = ssh.communicate()
        result = json.loads(output[0])
        if output[1]:
            error = ssh.stderr.readlines()
            log_to_postgres("ERROR: %s" % error, DEBUG)
        else:
            servers = result['servers']
            for server, values in servers.items():
                if self.table_name == 'server_config':
                    line = {
                        'server': server,
                        'description': values['config']['description'],
                        'config': json.dumps(values['config'])
                    }
                    yield line
                elif self.table_name =='server_status':
                    line = {'server': server}
                    for key, value in values['status'].items():
                        line[key] = value
                    yield line
                else:
                    server = server.replace('-', '_').replace('.', '_')
                    for backup, properties in values['backups'].items():

                        if server == self.table_name.replace('-',
                                                             '_').replace('.',
                                                                          '_'):

                            line = {}
                            for key in properties.keys():
                                line[key] = properties[key]
                            yield line

    @classmethod
    def import_schema(self, schema, srv_options, options, restriction_type,
                      restricts):
        """
        Hook called on an IMPORT FOREIGN SCHEMA command.

        """
        diagnose = "barman diagnose"
        barman_host = "%s@%s" %(srv_options['barman_user'],
                                srv_options['barman_host'])
        ssh = subprocess.Popen(["ssh", "%s" % barman_host, diagnose],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        output = ssh.communicate()
        result = json.loads(output[0])
        if output[1]:
            error = ssh.stderr.readlines()
            log_to_postgres("ERROR: %s" % error, DEBUG)
        else:
            servers = result['servers']
            tables = []
            for server, values in servers.items():
                server = server.replace('-', '_').replace('.', '_')
                log_to_postgres('schema %s table %s' % (schema, server), DEBUG)
                for backup, properties in values['backups'].items():
                    table = TableDefinition(table_name=server)
                    table.options['schema'] = schema
                    table.options['table_name'] = server
                    for key in properties.keys():
                        table.columns.append(
                            ColumnDefinition(column_name=key,
                                             type_oid=1043,
                                             type_name='character varying'
                                             ))
                    tables.append(table)

            table_config = TableDefinition(table_name='server_config')
            table_config.options['schema'] = schema
            table_config.options['table_name'] = 'server_config'
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
            tables.append(table_config)
            table_status = TableDefinition(table_name='server_status')
            table_status.options['schema'] = schema
            table_status.options['table_name'] = 'server_status'
            for server, values in servers.items():
                for key in values['status'].keys():
                    table_status.columns.append(
                        ColumnDefinition(
                            column_name=key,
                            type_name='character varying'
                        )
                    )
                break
            table_status.columns.append(
                ColumnDefinition(
                    column_name='server',
                    type_name='character varying'
                ))
            tables.append(table_status)
            return tables

    @property
    def rowid_column(self):
        return self._row_id_column

    def insert(self, new_values):
        """
        This method is invoked every time a SELECT is executed
        on the foreign table.

        """
        log_to_postgres('Barman FDW INSERT output:  %s' % new_values, DEBUG)
        backup_cmd = "barman backup %s" % new_values['server_name']

        log_to_postgres('Barman FDW INSERT output:  %s' % backup_cmd, DEBUG)
        ssh = subprocess.Popen(["ssh", '-A', "%s" % self.barman_host,
                                backup_cmd],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        output = ssh.communicate()

        log_to_postgres('Barman FDW INSERT output:  %s' % output[0], DEBUG)
        log_to_postgres('Barman FDW INSERT errors:  %s' % output[1], DEBUG)
        return new_values
