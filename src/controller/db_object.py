from controller import os_commands
from controller.helper import execute_bash
from controller.helper import need_sudo
from controller.plugin_exception import plugin_exception
from controller.config_meta import config_meta
from dlpx.virtualization.platform.exceptions import UserError
import logging
import os
import json

from mssql import db_commands

logger = logging.getLogger(__name__)


class db_object(object):
    """
    Class for managing databases
    Each database impementation should inherit from this class
    This class has a typical methods implemented


    This class or any child classes should by initialized by providing all vSDK
    objects via named parameters like this:
 
    db_object(staged_source=staged_source, repository=repository, source_config=source_config, etc)
    """

    def __init__(self, **kargs):
        """
        Class constructor
        Depends on workflow it's initialized with the following arguments
        For dSource:
            - staged_source
            - repository
            - source_config or snapshot
        For VDB:
            - virtual_source
            - repository
            - source_config or snapshot
        """
        logger.debug("initialize db_object")
        # save all init objects into config_meta object for standard access
        self.__config = config_meta(**kargs)
        # compare if repository owner is equal environment user executing commands and set sudo and uid property
        self.__sudo = need_sudo(
            self.__config.connection, self.__config.repository.uid, self.__config.repository.gid)
        self.__uid = self.__config.repository.uid
        # set config directory to mount_point/.config
        self.__config_path = os.path.join(self.__config.parameters.mount_path, '.config')
        # database files path set directory to mount_point/db
        self.__db_path = os.path.join(self.__config.parameters.mount_path, 'db')


    @property
    def config(self):
        """
        define class property and return a config object
        """
        return self.__config


    @property
    def sudo(self):
        """
        define class property and return a __sudo property
        """
        return self.__sudo

    @property
    def config_path(self):
        """
        define class property and return a __config_path property
        """
        return self.__config_path

    @property
    def db_path(self):
        """
        define class property and return a __db_path property
        """
        return self.__db_path

    @property
    def uid(self):
        """
        define class property and return a __uid property
        """
        return self.__uid


    def get_db_name(self):
        """
        Return a database name - defined for staging database if called for dSource
        or virtual database 
        """
        logger.debug("get_db_name")
        if self.config.dSource:
            dbname = self.config.source_config.database_name
        else:
            dbname = self.config.virtual_source.parameters.database_name
        return dbname


    def save_json(self, file_path, obj):
        """
        Save obj (directory) as JSON file using a file_path
        Raise plugin_exception if something is wrong
        """
        logger.debug("save_json")
        # add exception
        obj_json = json.dumps(obj)
        obj_json = obj_json.replace('"', '\\"')
        logger.debug("fixed object: {}".format(obj_json))
        
        cmd = os_commands.write_file(file_path, obj_json, self.sudo, self.uid)
        write_file = execute_bash(source_connection=self.config.connection, command_name=cmd)
        if write_file.exit_code != 0:
            logger.debug("Can't write obj to file {}".format(file_path))
            raise plugin_exception(write_file)


    def load_json(self, file_path):
        """
        Save obj (directory) as JSON file using a file_path
        Raise plugin_exception if something is wrong
        """
        logger.debug("load_json")
        cmd = os_commands.read_file(file_path, self.sudo, self.uid)
        read_file = execute_bash(source_connection=self.config.connection, command_name=cmd)
        if read_file.exit_code != 0:
            logger.debug("Can't read from file {}".format(file_path))
            raise plugin_exception(read_file)

        # add exception
        obj = json.loads(read_file.stdout)
        return obj


    def run_db_command(self, command_name, **kargs):
        """
        Run a database command provided by command_name
        kargs is a set of additional parameters provided for command_name
        Command name has to be defied in db_commands module

        Standard parameters like: password, username, client_path 
        are automatically set by this function
        """
        logger.debug("run_db_command")
        client_path = self.config.repository.client_path
        if self.config.dSource:
            username = self.config.staged_source.parameters.instance_user
            password = self.config.staged_source.parameters.instance_password
        else:
            username = self.config.virtual_source.parameters.instance_user
            password = self.config.virtual_source.parameters.instance_password

        env = {"SQLCMDPASSWORD": password}
        method_to_call = getattr(db_commands, command_name)
        cmd = method_to_call(client_path=client_path,
                             username=username, **kargs)
        sqlcommand = execute_bash(
            source_connection=self.config.connection, command_name=cmd, environment_vars=env)

        if sqlcommand.exit_code != 0:
            raise plugin_exception(sqlcommand)

        to_list = map(lambda x: x.strip(), sqlcommand.stdout.split("\n"))
        return to_list


    def db_exist(self):
        """
        Check if database exist
        It's using a list_db_names method from db_commands module
        and check an output
        """
        logger.debug("db_exist")
        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()
        logger.debug("checkig if db {} exists in instance".format(dbname))

        try:
            list_of_database = self.run_db_command("list_db_names")
        except plugin_exception as p:
            raise UserError("Error reading list of database from instance", action="Check output for detailed error",
                            output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

        if dbname in list_of_database:
            return True
        else:
            return False

    def check_vdb(self):
        """
        Check status of staging database or VDB
        It's using a get_mssql_databases_status method from db_commands class
        and return an output
        """
        logger.debug("check_vdb_status")
        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()
        try:
            status = self.run_db_command(
                "get_mssql_databases_status", database=dbname)
            return status[0]
        except plugin_exception as p:
            logger.debug("problem with monitoring")
            return "OFFLINE"

    def offline_vdb(self):
        """
        Bring a VDB offline
        It's using a offline_mssql method from db_commands class
        """
        logger.debug("offline_vdb")
        dbname = self.get_db_name()
        try:
            offline = self.run_db_command("offline_mssql", database=dbname)
        except plugin_exception as p:
            raise UserError("Problem with offlining database {}".format(
                dbname), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

    def online_vdb(self):
        """
        Bring a VDB online
        It's using a online_mssql method from db_commands class
        """
        logger.debug("online_vdb")
        dbname = self.get_db_name()
        try:
            online = self.run_db_command("online_mssql", database=dbname)
        except plugin_exception as p:
            raise UserError("Problem with onlining database {}".format(
                dbname), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))
