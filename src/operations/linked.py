#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging
import sys
import traceback

from generated.definitions import SnapshotDefinition
from dlpx.virtualization.platform import Mount, MountSpecification, Plugin, OwnershipSpecification
from dlpx.virtualization.platform import Status
from mssql.mssql_ctl import mssql_ctl
from controller.plugin_exception import plugin_exception


logger = logging.getLogger(__name__)


def staging_mount_point(staged_source, repository):
    """
    Staging mount method
    Mounting Delphix file system using a repository uid/gid ownership
    """
    mount_path = staged_source.parameters.mount_path
    environment = staged_source.staged_connection.environment
    logger.debug("Mounting path {}".format(mount_path))
    mounts = [Mount(environment, mount_path)]
    logger.debug("Setting ownership to uid {} and gid {}".format(
        repository.uid, repository.gid))
    ownership_spec = OwnershipSpecification(repository.uid, repository.gid)
    return MountSpecification(mounts, ownership_spec)


def resync(staged_source, repository, source_config):
    """
    Resync operation run for Resync API call (initial ingestion or force full backup)
    """
    logger.debug("In resync")

    # create an object of database
    db_object = mssql_ctl(staged_source=staged_source,
                          repository=repository, source_config=source_config)

    # create required directories
    db_object.create_staging_dirs()
    # get backup
    (backup_file, position, last_backup) = db_object.get_backup_file_to_restore(resync=True)

    if db_object.db_exist():
        # if staging database exists, drop and resore from backup
        db_object.drop_db()
        db_object.restore_database_from_backup(backup_file, position)
    else:
        # if staging database doesn't exist, resore from backup
        db_object.restore_database_from_backup(backup_file, position)

    # save additional information into JSON file to use them in post_snapshot
    db_object.save_backup_info(last_backup, backup_file, position)


def pre_snapshot(staged_source, repository, source_config):
    """
    Presnapshot operation run for normal snapshots (see plugin_runner.py)
    """
    logger.info("In pre_snapshot")

    # create an object of database
    db_object = mssql_ctl(staged_source=staged_source,
                          repository=repository, source_config=source_config)
    # create required directories
    db_object.create_staging_dirs()
    # get backup
    (backup_file, position, last_backup) = db_object.get_backup_file_to_restore(resync=False)

    if db_object.db_exist():
        # if staging database doesn't exist, resore from backup
        db_object.restore_database_from_backup(backup_file, position)
    else:
        # add code to handle what to do if stating database doesn't exist
        pass

    # save additional information into JSON file to use them in post_snapshot
    db_object.save_backup_info(last_backup, backup_file, position)


def post_snapshot(staged_source, repository, source_config):
    """
    Post_snapshot operation run after restore
    It will save a snapshot metadata for VDB's
    """
    logger.info("In post_snapshot")

    try:
        # create an object of database
        db_object = mssql_ctl(staged_source=staged_source,
                              repository=repository, source_config=source_config)
        # get list of database files to save in snapshot
        list_of_dbfile = db_object.get_file_list()
        # read data from JSON to get backup info
        last_backup = db_object.load_last_backup()
        # define snapshot
        snap = SnapshotDefinition(db_files=list_of_dbfile, backup_time=last_backup["backup"])
        logger.debug(snap)
        # return snapshot object
        return snap

    except Exception:
        # IF SHOULD ALWAYS return a snapshot definition otherwise we can hit DLPX-74381
        # Check with team if we can define a warning inside snapshot
        ttype, value, tracebt = sys.exc_info()
        logger.debug("General exception handing in linked.post_snapshot")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(traceback.traceback.format_exc())
        return SnapshotDefinition(db_files=[])


def staging_status(staged_source, repository, source_config):
    """
    statgin_status check a status of the staging
    """
    try:
        # create an object of database
        db_object = mssql_ctl(staged_source=staged_source,
                              repository=repository, source_config=source_config)
        # check status of the VDB using SQL from db commands
        status = db_object.check_vdb()
        logger.debug("Value of status variable is #{}#".format(status))
        # compare status from SQL command and return status from Status class
        if status == "ONLINE":
            logger.debug("Staging database status is:  {}".format("ACTIVE"))
            return Status.ACTIVE
        else:
            logger.debug("Staging database status is:  {}".format("INACTIVE"))
            return Status.INACTIVE
    except plugin_exception:
        raise
    except Exception:
        ttype, value, traceb = sys.exc_info()
        logger.debug("General exception handing in linked.status_vdb")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(traceback.format_exc())
        return Status.INACTIVE


def start_staging(staged_source, repository, source_config):
    """
    start_staging operation run on enable dSource
    """

    try:
        logger.debug("start_staging")
        # create an object of database
        db_object = mssql_ctl(staged_source=staged_source,
                              repository=repository, source_config=source_config)
        # start staging server
        db_object.start_staging()
    except Exception:
        ttype, value, traceb = sys.exc_info()
        logger.debug("General exception handing in virtual.stop_vdb")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(traceback.format_exc())
        return db_object.online_vdb()


def stop_staging(staged_source, repository, source_config):
    """
    stop_staging operation run on disable or delete dSource
    """

    try:
        logger.debug("stop_staging")
        # create an object of database
        db_object = mssql_ctl(staged_source=staged_source,
                              repository=repository, source_config=source_config)
        # stop staging server
        db_object.stop_staging()
    except Exception:
        ttype, value, traceb = sys.exc_info()
        logger.debug("General exception handing in virtual.stop_vdb")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(traceback.format_exc())
        return db_object.offline_vdb()
