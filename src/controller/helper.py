#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging
import re

from dlpx.virtualization import libs
from dlpx.virtualization.libs import exceptions
from dlpx.virtualization.common import RemoteEnvironment
from dlpx.virtualization.common import RemoteHost
from dlpx.virtualization.common import RemoteUser
from dlpx.virtualization.common import RemoteConnection
from dlpx.virtualization.libs import PlatformHandler

from controller.os_command_response import OS_Command_Response
from controller import os_commands

# logger object
logger = logging.getLogger(__name__)

def setup_logger():
    """ 
    Setup logger for plugin
    """
    log_message_format = '[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s'
    log_message_date_format = '%Y-%m-%d %H:%M:%S'

    # Create a custom formatter. This will help in diagnose the problem.
    formatter = logging.Formatter(log_message_format, datefmt=log_message_date_format)

    platform_handler = libs.PlatformHandler()
    platform_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.addHandler(platform_handler)

    # The root logger's default level is logging.WARNING.
    # Without the line below, logging statements of levels
    # lower than logging.WARNING will be suppressed.
    logger.setLevel(logging.DEBUG)


def find_ids(source_connection, install_path):
    """ 
    return the repository uid and gid
    """
    cmd = os_commands.get_ids(install_path)
    ids = execute_bash(source_connection, cmd)
    ids = re.search(r"[-rwx.]+\s\d\s([\d]+)\s([\d]+).*", ids.stdout)
    if ids:
        uid = int(ids.group(1))
        gid = int(ids.group(2))
    else:
        uid = -1
        gid = -1
    logger.debug("repository user uid {} gid {}".format(uid, gid))
    return (uid, gid)


def make_nonprimary_connection(primary_connection, secondary_env_ref, secondary_user_ref):
    """
    Create a connection to 2nd server is VDB has more than one environment defined 
    """
    dummy_host = primary_connection.environment.host
    user = RemoteUser(name="unused", reference=secondary_user_ref)
    environment = RemoteEnvironment(name="unused", reference=secondary_env_ref, host=dummy_host)
    return RemoteConnection(environment=environment, user=user)


def execute_bash(source_connection, command_name, **kwargs ):
    """
    :param source_connection: Connection object for the source environment
    :param command_name: Command to be search from dictionary of bash command
    :param kwargs: Dictionary to hold key-value pair for this command
    :return: OS_Command_Response object (with stdout, stderr and exit_code)
    """

    if type(kwargs) != dict :
        raise exceptions.PluginScriptError("Parameters should be type of dictionary")

    if(source_connection is None):
        raise exceptions.PluginScriptError("Connection object cannot be empty")

    # Putting if block because in some cases, environment_vars is not defined in kwargs then we need to pass empty
    # dict. Otherwise it will raise Exception.
    if 'environment_vars' in kwargs.keys():
        environment_vars = kwargs['environment_vars']
        if type(environment_vars) != dict:
            raise exceptions.PluginScriptError("environment_vars should be type of dictionary. Current type is{}".format(type(environment_vars)))
    else:
        #making empty environment_variable for this command
        environment_vars = {}


    logger.debug("Bash command: {}".format(command_name))
    result = libs.run_bash(source_connection, command=command_name, variables=environment_vars, use_login_shell=True)

    return OS_Command_Response(result.stdout, result.stderr, result.exit_code)



def decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = decode_list(item)
        elif isinstance(item, dict):
            item = decode_dict(item)
        rv.append(item)
    return rv

def decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = decode_list(value)
        elif isinstance(value, dict):
            value = decode_dict(value)
        rv[key] = value
    return rv


def need_sudo(source_connection, repo_uid, repo_gid):
    """ 
    Check if user executing a command is a repository owner
    and return true/false
    """
    (uid, gid) = find_whoami(source_connection)
    if uid != repo_uid or gid != repo_gid:
        return True
    else:
        return False


def find_whoami(source_connection):
    """ 
    return the user env id
    """

    who = execute_bash(source_connection, os_commands.whoami())
    logger.debug("find whoami output: {}".format(who.stdout))
    ids = re.search(r"uid=([\d]+).*gid=([\d]+)", who.stdout)
    if ids:
        uid = int(ids.group(1))
        gid = int(ids.group(2))
    else:
        uid = -1
        gid = -1
    logger.debug("Delphix user uid {} gid {}".format(uid, gid))
    return (uid, gid)