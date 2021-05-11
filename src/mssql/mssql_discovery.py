#
# Copyright (c) 2020 by Delphix. All rights reserved.
#


import logging
import os
import re

# Delphix imports
from dlpx.virtualization.platform.exceptions import PluginRuntimeError
from dlpx.virtualization.platform.exceptions import UserError
from generated.definitions import (
    RepositoryDefinition,
    SourceConfigDefinition
)

# plugin imports

from controller import os_commands
from controller.helper import execute_bash
from controller.helper import find_ids

logger = logging.getLogger(__name__)


MSSQL_NICE_NAMES = {
    '14.0': '2017',
    '15.0': '2019'
}




def return_repository(connection):
    """
    Platform depended code to return a information about repository ( MS SQL installation )
    """
    repo_list = []
    
    logger.debug("finding a mssql-server packages")
    cmd = os_commands.rpm(package_name="mssql-server")
    server_pkg = execute_bash(source_connection=connection, command_name=cmd)

    if server_pkg.exit_code != 0:
        # add logger
        UserError("Problem with finding a mssql server RPM package", output="stdout: {} stderr: {}".format(server_pkg.stdout, server_pkg.stderr))

    logger.debug("finding a mssql-client packages")
    cmd = os_commands.rpm(package_name="mssql-tools")
    client_pkg = execute_bash(source_connection=connection, command_name=cmd)

    if client_pkg.exit_code != 0:
        UserError(message="Problem with finding a mssql client RPM package", output="stdout: {} stderr: {}".format(client_pkg.stdout, client_pkg.stderr))

    logger.debug("finding a mssql-client path")
    cmd = os_commands.check_directory('/opt/mssql-tools/bin/')
    client_path = execute_bash(source_connection=connection, command_name=cmd) 

    if client_path.exit_code != 0:
        UserError(message="Problem with finding a mssql client path", output="stdout: {} stderr: {}".format(client_path.stdout, client_path.stderr))
    else:
        os_client_path = '/opt/mssql-tools/bin/'


    logger.debug("finding a mssql-server path")
    cmd = os_commands.check_directory('/opt/mssql/bin/')
    server_path = execute_bash(source_connection=connection, command_name=cmd) 

    if server_path.exit_code != 0:
        UserError(message="Problem with finding a mssql client path", output="stdout: {} stderr: {}".format(server_path.stdout, server_path.stderr))
    else:
        os_server_path = '/opt/mssql/bin/'

    # for now only hardcoded discovery
    (uid, gid) = find_ids(connection, '/var/opt/mssql/mssql.conf')

    # check an instance name
    logger.debug("finding a mssql-server hostname")
    cmd = os_commands.get_hostname(short=True)
    hostname = execute_bash(source_connection=connection, command_name=cmd) 

    if hostname.exit_code != 0:
        UserError(message="Problem with finding a mssql hostname", output="stdout: {} stderr: {}".format(hostname.stdout, hostname.stderr))
    else:
        os_hostname = hostname.stdout.strip()

    # each mssql-server package can be a repository

    list_of_servers = server_pkg.stdout.split('\n')

    for server in list_of_servers:
        version = re.search(r"mssql-server-([\d]+.\d).([\d]+.[\d]+)", server)
        if version:
            main_version = version.group(1)
            rc_version = version.group(2)
            repo = RepositoryDefinition(version='{} {}'.format(main_version, rc_version), rdbms_path=os_server_path, gid=gid, client_path=os_client_path, 
                                        pretty_name='MSSQL Linux - {} (version: {})'.format(os_hostname, MSSQL_NICE_NAMES[main_version]), uid=uid)
            logger.debug(repo)
            repo_list.append(repo)



    return repo_list