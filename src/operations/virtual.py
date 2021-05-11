#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging
import time
import sys
import traceback

from generated.definitions import (
    RepositoryDefinition,
    SourceConfigDefinition,
    SnapshotDefinition
)
from dlpx.virtualization.platform import Mount, MountSpecification, Plugin, OwnershipSpecification
from dlpx.virtualization.platform.exceptions import UserError
from dlpx.virtualization.platform import Status
from mssql.mssql_ctl import mssql_ctl

logger = logging.getLogger(__name__)

"""
This file contains a list of plugin operations for VDB
"""


def virtual_mount_specification(virtual_source, repository):
    """
    Virtual mount method
    Mounting Delphix file system using a repository uid/gid ownership
    """
    mount_path = virtual_source.parameters.mount_path
    environment = virtual_source.connection.environment
    logger.debug("Mounting path {}".format(mount_path))
    mounts = [Mount(environment, mount_path)]
    logger.debug("Setting ownership to uid {} and gid {}".format(
        repository.uid, repository.gid))
    ownership_spec = OwnershipSpecification(repository.uid, repository.gid)
    return MountSpecification(mounts, ownership_spec)


def configure(virtual_source, snapshot, repository):
    """
    Run for provision and refresh of the VDB
    """

    logger.debug("in virtual configure")

    # create an object of database
    db_object = mssql_ctl(virtual_source=virtual_source,
                          repository=repository, snapshot=snapshot)

    # create required directories
    db_object.create_staging_dirs()

    # check if database already exist
    if db_object.db_exist():
        # raise a User Error and stop a job
        raise UserError("DB with name {} exist in instance. Can't provision VDB.".format(
            db_object.get_db_name()))
    else:
        # call method to create a new VDB
        db_object.create_vdb()

    # define source config data and return it to save inside Delphix
    sourceconfig = SourceConfigDefinition(
        database_name=db_object.get_db_name())
    return sourceconfig


def post_snapshot(virtual_source, repository, source_config):
    """
    Post_snapshot operation run after every VDB snapshot
    It will save a snapshot metadata
    """
    logger.info("In post_snapshot")

    try:
        # create an object of database
        db_object = mssql_ctl(virtual_source=virtual_source,
                              repository=repository, source_config=source_config)
        # get information to save inside snapshot
        # schema depended 
        list_of_dbfile = db_object.get_file_list()
        # Create snapshot definition 
        snap = SnapshotDefinition(db_files=list_of_dbfile)
        logger.debug(snap)
        return snap

    except Exception:
        # IF SHOULD ALWAYS return a snapshot definition otherwise we can hit DLPX-74381
        # Check with team if we can define a warning inside snapshot
        ttype, value, traceb = sys.exc_info()
        logger.debug("General exception handing in virtual.post_snapshot")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(traceback.format_exc())
        return SnapshotDefinition(db_files=[])


def start_vdb(virtual_source, repository, source_config):
    """
    start_vdb operation run on start and enable of the VDB
    """
    logger.debug("start_vdb")
    try:
        # create an object of database
        db_object = mssql_ctl(virtual_source=virtual_source,
                              repository=repository, source_config=source_config)
        # call a online method
        db_object.online_vdb()
    except plugin_exception:
        # pass a plugin exception
        raise
    except UserError:
        # pass a plugin exception
        raise 
    except Exception:
        # Catch all other exceptions
        ttype, value, traceb = sys.exc_info()
        logger.debug("General exception handing in virtual.start_vdb")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(traceback.format_exc())




def vdb_status(virtual_source, repository, source_config):
    """
    vdb_status check a status of the VDB
    """
    try:
        # create an object of database
        db_object = mssql_ctl(virtual_source=virtual_source,
                              repository=repository, source_config=source_config)

        # status returned should be ONLINE or OFFLINE
        status = db_object.check_vdb()
        logger.debug("Value of status variable is: {}".format(status))
        if status == "ONLINE":
            return Status.ACTIVE
        else:
            return Status.INACTIVE
    except plugin_exception:
        # pass a plugin exception
        raise
    except UserError:
        # pass a plugin exception
        raise 
    except Exception:
        ttype, value, traceb = sys.exc_info()
        logger.debug("General exception handing in virtual.status_vdb")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(traceback.format_exc())
        return Status.INACTIVE


def stop_vdb(virtual_source, repository, source_config):
    """
    stop_vdb operation run on stop, rewind, refresh and disable of the VDB
    """
    logger.debug("stop_vdb")
    try:
        # create an object of database
        db_object = mssql_ctl(virtual_source=virtual_source,
                              repository=repository, source_config=source_config)
        db_object.offline_vdb()
    except plugin_exception:
        # pass a plugin exception
        raise
    except UserError:
        # pass a plugin exception
        raise 
    except Exception:
        ttype, value, traceb = sys.exc_info()
        logger.debug("General exception handing in virtual.stop_vdb")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(traceback.format_exc())


def reconfigure(virtual_source, repository, source_config, snapshot):
    """
    reconfigure operation run on rewind operation after unconfigure
    """
    logger.debug("in reconfigure")

    # create an object of database
    db_object = mssql_ctl(virtual_source=virtual_source, repository=repository,
                          snapshot=snapshot, source_config=source_config)

    # create required directories
    db_object.create_staging_dirs()

    # check if database already exist
    if db_object.db_exist():
        # raise error is yes
        # potential handling should be here as well
        raise UserError("DB with name {} exist in instance. Can't provision VDB.".format(
            db_object.get_db_name()))
    else:
        # recreate VDB after rewind
        db_object.attach_vdb()

    # create a new source config object
    sourceconfig = SourceConfigDefinition(
        database_name=db_object.get_db_name())
    return sourceconfig


def unconfigure(virtual_source, repository, source_config):
    """
    unconfigure operation run on disable, rewind, delete and refresh operation
    """
    logger.debug("in virtual unconfigure")

    # create an object of database
    db_object = mssql_ctl(virtual_source=virtual_source,
                          repository=repository, source_config=source_config)

    if db_object.db_exist():
        # we can't delete as this will delete data
        # just detach is enough as data files are needed to enable
        db_object.detach_db()
    else:
        logger.debug("Database doesn't exist in instance - good to go")


def pre_snapshot(virtual_source, repository, source_config):
    """
    pre_snapshot operation run before snapshot of VDB
    potenial flush / sync operations for VDB should be implemented here
    """
    logger.info("In virtual pre_snapshot")
    
