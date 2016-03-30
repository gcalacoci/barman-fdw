import json
import subprocess
from logging import ERROR, DEBUG

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres


class BarmanForeignDataWrapper(ForeignDataWrapper):
    def __init__(self, options, columns):
        """
        Init method for the Foreign Data Wrapper.

        Used to manage the options necessary to run barman

        :type options: Options passed during the creation of the FDW
        :type columns: the columns of the foreign table
        """
        super(BarmanForeignDataWrapper, self).__init__(options, columns)

        self._row_id_column = 'server'

        # The columns we'll be using (defaults to 'all'):
        self.columns = columns

        log_to_postgres('Barman FDW Config options:  %s' % options, DEBUG)
        log_to_postgres('Barman FDW Config columns:  %s' % columns, DEBUG)

        if options.has_key('barman_host'):
            self.barman_host = options['barman_host']
        else:
            log_to_postgres('Option barman_host is required for '
                            'the Barman FDW setup.', ERROR)

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

        ssh = subprocess.Popen(["ssh", "%s" % self.barman_host, diagnose],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        output = ssh.communicate()
        result = json.loads(output[0])
        if output[1]:
            error = ssh.stderr.readlines()
            log_to_postgres("ERROR: %s" % error)
        else:
            servers = result['servers']
            for server, values in servers.iteritems():
                line = {
                    'server': server,
                    'backups': len(values['backups']),
                    'description': values['config']['description'],
                    'config': json.dumps(values['config'])
                }
                yield line

    @property
    def rowid_column(self):
        return self._row_id_column

    def insert(self, new_values):
        """
        This method is invoked every time a SELECT is executed
        on the foreign table.

        """
        log_to_postgres('Barman FDW INSERT output:  %s' % new_values )
        backup_cmd = "barman backup %s" % new_values['server']

        log_to_postgres('Barman FDW INSERT output:  %s' % backup_cmd )
        ssh = subprocess.Popen(["ssh", '-A', "%s" % self.barman_host, backup_cmd],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        output = ssh.communicate()

        log_to_postgres('Barman FDW INSERT output:  %s' % output[0] )
        log_to_postgres('Barman FDW INSERT errors:  %s' % output[1] )
        return new_values
