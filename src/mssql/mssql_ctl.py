#
# Copyright (c) 2020 by Delphix. All rights reserved.
#


import logging
import os
import re
import json
import ntpath
import datetime

from controller.config_meta import config_meta
from dlpx.virtualization.platform import Status
from dlpx.virtualization.platform.exceptions import PluginRuntimeError
from dlpx.virtualization.platform.exceptions import UserError
from mssql import db_commands
from controller import os_commands
from controller.helper import execute_bash
from controller.helper import need_sudo
from controller.plugin_exception import plugin_exception
from controller.db_object import db_object


logger = logging.getLogger(__name__)


class mssql_ctl(db_object):
    """
    Class for managing Cassandra clusters
    It has methods for cluster wide operations like start, stop, create or restore
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
        logger.debug("initialize mssql_ctl")
        # Initializing the parent class constructor
        super(mssql_ctl, self).__init__(**kargs)
        self.__seed_path = os.path.join(self.config.parameters.mount_path, '.seed')

    @property
    def seed_path(self):
        return self.__seed_path


    def apply_fresh_backups(self, client_path, username, backup_path, backup_location):
        last_applied = self.run_db_command("get_mssql_backup_infos")
        last_applied = last_applied.split('/')
        last_applied = last_applied[0].strip()
        last_applied = datetime.strptime(last_applied, '%Y-%m-%d %H:%M:%S.%f')
        new_backups = self.run_db_command("get_last_backup")
        for backup in new_backups:
            for timestamp, backup_file in backup.items():
                timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S%f')
                if timestamp > last_applied:
                    #       print "Backup to be applied : " + backup_file
                    backup = "Full"
                    if backup == "Full":
                        print("call <restore backup...> function")
                    elif backup == "Transaction log":
                        print("call <restore log...> function ")
                    elif backup == "Differential database":
                        print("call <restore backup...>function")
                    else:
                        print("not yet supported")

        return False


    def get_backup_file_info(self, backup_path):
        logger.debug("get_backup_file_info")
        filelist = []

        try:
            filesinfo = self.run_db_command(
                "get_backup_file_info", backup_path=backup_path)
        except plugin_exception as p:
            raise UserError("Restore filelist failure from path {}".format(
                backup_path), action="Check if you are restoring valid backup", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

        for line in filesinfo:
            cols = line.split('|')
            (logicalfilename, physicalfilename,
             filetype, filegroupname) = cols[:4]
            fileid = cols[6]
            filedef = {
                "logicalname": logicalfilename.strip(),
                "physicalname": ntpath.basename(physicalfilename.strip()),
                "filetype": filetype.strip(),
                "groupname": filegroupname.strip(),
                "fileid": int(fileid.strip())
            }
            logger.debug(filedef)
            filelist.append(filedef)
        return filelist

    def get_db_file_info(self):
        logger.debug("get_backup_file_info")

        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()
        try:
            filesinfo = self.run_db_command(
                "get_mssql_databases_fileinfo", database=dbname)
        except plugin_exception as p:
            raise UserError("Reading data file list from database {} failed".format(
                dbname), action="Check if this is a valid database and if credentials are OK", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))
        filelist = []
        for line in filesinfo:
            cols = line.split('|')
            (logicalfilename, fileid, physicalfilename,
             filetype, filegroupname) = cols
            filedef = {
                "logicalname": logicalfilename.strip(),
                "physicalname": ntpath.basename(physicalfilename.strip()),
                "filetype": filetype.strip(),
                "groupname": filegroupname.strip(),
                "fileid": int(fileid.strip())
            }
            logger.debug(filedef)
            filelist.append(filedef)
        return filelist

    def generate_move(self, x):
        return "MOVE '{}' TO '{}'".format(x["logicalname"], os.path.join(self.db_path, x["physicalname"]))

    def get_backup_headers(self, filename):
        backup_dict = {
            "1": "Full",
            "2": "Transaction log",
            "4": "File",
            "5": "Differential database",
            "6": "Differential file",
            "7": "Partial",
            "8": "Differential partial"
        }

        backup_path = os.path.join(self.config.parameters.backup_location, filename)

        try:
            header_list = self.run_db_command("get_backup_headers", backup_path=backup_path)
        except plugin_exception as p:
            raise UserError("Reading data from backup file {} failed".format(backup_path), action="Checkout output for details", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

        backup_list = []

        #  BackupName|BackupDescription|BackupType|ExpirationDate|Compressed|Position|DeviceType|UserName|ServerName|DatabaseName|DatabaseVersion|DatabaseCreationDate
        # |BackupSize|FirstLSN|LastLSN|CheckpointLSN|DatabaseBackupLSN|BackupStartDate|BackupFinishDate|SortOrder|CodePage|UnicodeLocaleId|UnicodeComparisonStyle
        # |CompatibilityLevel|SoftwareVendorId|SoftwareVersionMajor|SoftwareVersionMinor|SoftwareVersionBuild|MachineName|Flags|BindingID|RecoveryForkID|Collation
        # |FamilyGUID|HasBulkLoggedData|IsSnapshot|IsReadOnly|IsSingleUser|HasBackupChecksums|IsDamaged|BeginsLogChain|HasIncompleteMetaData|IsForceOffline|IsCopyOnly
        # |FirstRecoveryForkID|ForkPointLSN|RecoveryModel|DifferentialBaseLSN|DifferentialBaseGUID|BackupTypeDescription|BackupSetGUID|CompressedBackupSize|Containment
        # |KeyAlgorithm|EncryptorThumbprint|EncryptorType



        for backup in header_list:
            cols = backup.split('|')
            header = {
                "backup_type": backup_dict[cols[2].strip()],
                "position": int(cols[5].strip()),
                "database_name": cols[9].strip(),
                "backup_start_date": cols[17].strip(),
                "filename": filename
            }
            backup_list.append(header)

        return backup_list


    def drop_db(self):
        logger.debug("drop_db")

        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()

        try:
            filesinfo = self.run_db_command("drop_database", database=dbname)
        except plugin_exception as p:
            raise UserError("Dropping database {} failed".format(
                dbname), action="Check if you are restoring valid backup", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))


    def load_last_backup(self):
        try:
            filename = os.path.join(self.config_path, "{}_last_backup.json".format(self.get_db_name()))
            last_backup = self.load_json(filename)
        except plugin_exception as p:
            # if there is no file - first restore
            # just ignore it
            last_backup = None

        return last_backup

    def get_backup_file_to_restore(self, resync):
        logger.debug("get_backup_file_to_restore")


        backup_dir = os.path.join(self.config.parameters.backup_location, self.config.parameters.backup_pattern)

        cmd = os_commands.list_dir(backup_dir, self.sudo, self.config.repository.uid)
        logger.debug("list backup cmd: {}".format(cmd))
        backup_dir_output = execute_bash(
            source_connection=self.config.connection, command_name=cmd)
	if backup_dir_output.exit_code != 0:
            raise UserError("Unable to list the files in the backup directory {}".format(
                backup_dir), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(
			backup_dir_output.stdout, backup_dir_output.stderr))


        backup_file_list = []

        match_file = re.compile(r'[-rwx.]+\s\d\s[\w]+\s[\w]+[\s]+[\d]+\s\d\d\d\d-\d\d-\d\d\s\d\d:\d\d:\d\d\s([\S]*)')

        for f in backup_dir_output.stdout.split("\n"):
            filename = re.search(match_file, f)
            if filename:
                backup_file_list.append(filename.group(1))


        logger.debug(backup_file_list)        

        if resync:
            # for resync we need to find a full backup

            backup_list = []

            for filename in backup_file_list:
                backup_list = backup_list + self.get_backup_headers(filename)
                logger.debug("backup header list: {}".format(backup_list))

            # list full backup only and sort by position - is sorting necessary ?
            full_backups = sorted([ x for x in backup_list if x["backup_type"] == "Full" ], key=lambda x: x["backup_start_date"])

            # take the newest full backup and save a position no
            position = full_backups[-1]["position"]
            filename = full_backups[-1]["filename"]
            last_backup = full_backups[-1]["backup_start_date"]
            logger.debug("restore filename: {}".format(filename))
            logger.debug("restore position: {}".format(position))

        else:
            # not resync - should it recover a last diff or all diffs ?
            logger.debug("not resync")

            backup_list = []

            for filename in backup_file_list:
                backup_list = backup_list + self.get_backup_headers(filename)
                logger.debug("backup header list: {}".format(backup_list))

            # diff backup has to be after last full backup
            # if there is only diff backup in dir - nothing we can do
            # if there is a full and diff backups in dir - we need to apply full first
            # but only if this is newer is last applied full

            last_backup = self.load_last_backup()


            if last_backup:

                # check if there is a new full backup
                logger.debug("Looking for full backups newer than {}".format(last_backup["backup"]))
                full_backups = sorted([ x for x in backup_list if x["backup_type"] == "Full" and x["backup_start_date"] > last_backup["backup"]  ], key=lambda x: x["backup_start_date"])
                logger.debug("List of full backups: {}".format(full_backups))
                if len(full_backups) > 0:
                    logger.debug("Newer full backup found")
                    position = full_backups[-1]["position"]
                    filename = full_backups[-1]["filename"]
                    last_backup = full_backups[-1]["backup_start_date"]
                    logger.debug("restore filename: {}".format(filename))
                    logger.debug("restore position: {}".format(position))
                else:
                    logger.debug("No new full backups - filter on diff")
                    logger.debug("Looking for diff backups newer than {}".format(last_backup["backup"]))
                    diff_backups = sorted([ x for x in backup_list if x["backup_type"] == "Differential database" and x["backup_start_date"] > last_backup["backup"]  ], key=lambda x: x["backup_start_date"])
                    logger.debug("List of diff backups: {}".format(diff_backups))
                    if len(diff_backups) > 0:
                        logger.debug("Newer diff backup found")
                        position = diff_backups[-1]["position"]
                        filename = diff_backups[-1]["filename"]
                        last_backup = diff_backups[-1]["backup_start_date"]
                        logger.debug("restore filename: {}".format(filename))
                        logger.debug("restore position: {}".format(position))
                    else:
                        raise UserError("No new backup found", action="Please check the backup location and verify there is a new backup. If you are certain there is a more recent backup, create a new set of support logs from the Delphix engine including plugin logs and review those.", output="N/A")


            else:
                backup_list = sorted(backup_list, key=lambda x: x["backup_start_date"])
                # take the newest backup and save a position no
                position = backup_list[-1]["position"]
                filename = backup_list[-1]["filename"]
                last_backup = backup_list[-1]["backup_start_date"]
                logger.debug("restore filename: {}".format(filename))
                logger.debug("restore position: {}".format(position))


        return (filename, position, last_backup)


    def save_backup_info(self, last_backup, filename, position):
        logger.debug("save_backup_info")
        obj = { 
            "backup": last_backup,
            "filename": filename,
            "position": position
        }
        try:
            filename = os.path.join(self.config_path, "{}_last_backup.json".format(self.get_db_name()))
            last_backup = self.save_json(filename, obj)
        except plugin_exception as p:
            # what to do here ?????
            logger.debug("save_backup_info failed")
            last_backup = None


    def restore_database_from_backup(self, backup_file, position):
        logger.debug("restore_database_from_backup")

        # if this is resync find newest full backup

        if self.config.parameters.for_upgrade:
            self.restore_database_from_backup_for_upgrade(backup_file, position)
        else:
            self.restore_database_from_backup_same_version(backup_file, position)

    def restore_database_from_backup_same_version(self, backup_file, position):
        logger.debug("restore_database_from_backup_same_version")
        dbname = self.get_db_name()
        backup_path = backup_file
        filelist = self.get_backup_file_info(backup_path)
        movelist = map(self.generate_move, filelist)
        logger.debug(movelist)
        try:
            filesinfo = self.run_db_command("restore_full_backup", backup_path=backup_path, mount_path=self.db_path,
                                            move=','.join(movelist), database=dbname, position=position)
        except plugin_exception as p:
            raise UserError("Restore from backup location {} failed".format(
                backup_path), action="Check if you are restoring valid backup", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

    def restore_database_from_backup_for_upgrade(self, backup_file, position ):
        logger.debug("restore_database_from_backup")
        dbname = self.get_db_name()
        backup_path = backup_file
        filelist = self.get_backup_file_info(backup_path)
        movelist = map(self.generate_move, filelist)
        logger.debug(movelist)

        try:
            filesinfo = self.run_db_command("restore_full_backup_for_upgrade", backup_path=backup_path, mount_path=self.db_path,
                                            move=','.join(movelist), database=dbname, position=position)
        except plugin_exception as p:
            raise UserError("Restore from backup location {} failed".format(
                backup_path), action="Check if you are restoring valid backup or if for_upgrade flag need to be set", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

    def get_file_list(self):
        """
        Return a list of file names to move from staging to VDB
        """
        logger.debug("get_file_list")
        if self.config.dSource:
            # this is called to list files - so snapshot should be done and last backup config file should be there as well
            try:
                filename = os.path.join(self.config_path, "{}_last_backup.json".format(self.get_db_name()))
                last_backup = self.load_json(filename)
            except plugin_exception as p:
                logger.debug("No config file so we have a problem - snapshot will be corrupted")
                return json.dumps({})

            backup_path = os.path.join(self.config.parameters.backup_location, last_backup["filename"])
            return json.dumps(self.get_backup_file_info(backup_path))
        else:
            logger.debug("extract files from database")
            return json.dumps(self.get_db_file_info())

    def create_vdb(self):
        logger.debug("create_vdb")

        # TODO
        # add error handling
        file_list = json.loads(self.config.snapshot.db_files)

        self.create_seed(file_list)
        self.offline_vdb()
        self.replace_files(file_list)
        self.online_vdb()

        # TODO
        # if all OK cleanup
        self.cleanup()

    def attach_vdb(self):
        logger.debug("attach_vdb")

        # TODO
        # add error handling
        file_list = json.loads(self.config.snapshot.db_files)
        file_list_attach = map(self.generate_attach_list, file_list)
        dbname = self.get_db_name()

        try:
            attachvdb = self.run_db_command(
                "attach_database", database=dbname, files=",".join(file_list_attach))

        except plugin_exception as p:
            raise UserError("Attached a VDB {} failed".format(
                dbname), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

    def generate_attach_list(self, x):
        return "(FILENAME='{}')".format(os.path.join(self.db_path, x["physicalname"]))

    def create_seed(self, file_list):
        logger.debug("create_seed")

        seed_path = self.seed_path

        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()

        cmd = os_commands.make_directory(
            seed_path, self.sudo, self.config.repository.uid)
        logger.debug("create directory cmd for seed: {}".format(cmd))
        command = execute_bash(
            source_connection=self.config.connection, command_name=cmd)

        primary_files = {x["filetype"]
            : x for x in file_list if x["fileid"] in [1, 2]}
        logger.debug(primary_files)

        filename = primary_files["D"]["logicalname"]
        physical_filename = os.path.join(
            seed_path, primary_files["D"]["physicalname"])
        logname = primary_files["L"]["logicalname"]
        physical_logname = os.path.join(
            seed_path, primary_files["L"]["physicalname"])

        try:
            createvdb = self.run_db_command("create_target_mssql_vdb", filename=filename, physical_filename=physical_filename,
                                            logname=logname, physical_logname=physical_logname, database=dbname)

        except plugin_exception as p:
            raise UserError("Creating a seed database {} failed".format(
                dbname), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

        rest_of_files = sorted([x for x in file_list if x["fileid"] not in [
                               1, 2]], key=lambda f: f["fileid"])

        set_of_filegroups = set(
            [x["groupname"] for x in rest_of_files if x["groupname"] != "NULL"])
        logger.debug("list of filegroups: {}".format(set_of_filegroups))

        try:

            for filegroup in set_of_filegroups:
                createfilegroup = self.run_db_command(
                    "create_filegroup", filegroup=filegroup, database=dbname)

        except plugin_exception as p:
            raise UserError("Adding a filegroup {} to seed database {} failed".format(
                filegroup, dbname), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

        # fileId of new created files needs to match a fileid from backup so we may need to add dummy files
        # end remove them at the end by using
        # DBCC SHRINKFILE (N'$files', EMPTYFILE)
        # ALTER DATABASE [$databaseName]  REMOVE FILE [$files]

        # FileId 1 and 2 are taken by primary so, we need to check if next file position minus fileid = 0

        local_fileid = 3  # this is a first file which should be added
        dummy_files = []

        try:
            for f in rest_of_files:
                fileid = int(f["fileid"])
                logger.debug("fileid {} local_fileid {} : {}".format(
                    fileid, local_fileid, fileid-local_fileid))
                if (fileid - local_fileid) != 0:
                    for fakeid in range(local_fileid, fileid):
                        logger.debug("adding dummy file {}".format(fakeid))
                        dummy_file_name = "dummy_{}".format(fakeid)
                        dummy_physical_name = os.path.join(
                            seed_path, dummy_file_name)
                        dummy_files.append(dummy_file_name)
                        createfile = self.run_db_command(
                            "add_datafile", database=dbname, filename=dummy_file_name, physical_filename=dummy_physical_name)
                        local_fileid = local_fileid + 1

                physical_filename = os.path.join(seed_path, f["physicalname"])
                if f["filetype"] == "D":
                    createfile = self.run_db_command(
                        "add_datafile_to_group", filegroup=f["groupname"], database=dbname, filename=f["logicalname"], physical_filename=physical_filename)
                elif f["filetype"] == "L":
                    createfile = self.run_db_command(
                        "add_logfile", database=dbname, logfile=f["logicalname"], physical_logfile=physical_filename)
                else:
                    logger.error("Unknown file type: {}".format(f))
                    raise UserError(
                        "Error with snapshot metadata - unknown file type", action="Contact Delphix", output=f)

                local_fileid = local_fileid + 1

        except plugin_exception as p:
            raise UserError("Adding a seed file {} to seed database {} failed".format(
                fileid, dbname), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

        try:
            # clean dummy files
            for dummy in dummy_files:
                shrink = self.run_db_command(
                    "shrink_datafile", database=dbname, filename=dummy)
                remove = self.run_db_command(
                    "drop_datafile", database=dbname, filename=dummy)
        except plugin_exception as p:
            raise UserError("Problem with cleaning up dummy datafile {}".format(
                dummy), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))



    def replace_files(self, file_list):
        logger.debug("replace_files")

        seed_path = self.seed_path
        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()

        try:
            for f in file_list:
                physical_filename = os.path.join(
                    self.db_path, f["physicalname"])
                rename = self.run_db_command(
                    "rename_datafile", filename=f["logicalname"], physical_filename=physical_filename, database=dbname)
        except plugin_exception as p:
            raise UserError("Problem with renaming file {}".format(
                f["logicalname"]), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

    def cleanup(self):
        logger.debug("cleanup")

        seed_path = self.seed_path
        dbname = self.get_db_name()

        cmd = os_commands.delete_dir(
            seed_path, True, self.sudo, self.config.repository.uid)
        logger.debug("create directory cmd for seed: {}".format(cmd))
        command = execute_bash(
            source_connection=self.config.connection, command_name=cmd)

        if command.exit_code != 0:
            raise UserError("Problem with cleaning up seed directory {}".format(
                seed_path), action="Please check output for detailed error", output="stdout: {}\nstderr: {}".format(command.stdout, command.stderr))


    def start_staging(self):
        """
        if staging is restored from same version of MS SQL, it was detached on stop
        check for_upgrade what to do
        if backup is from previous - do nothing
        for same version, there is no way to attach database standby database
        so same trick with seed database, plus backup / restore has to be applied
        """

        logger.debug("start_staging")

        if self.config.parameters.for_upgrade:
            logger.debug("for upgrade flag is set - do nothing")
            return


        file_list = self.get_file_list_from_file()
        self.cleanup()
        self.create_seed(file_list)
        self.backup_database()
        self.drop_db()
        self.restore_seed_database()
        self.replace_files(file_list)
        self.switch_to_standby()
        self.cleanup()


    def create_staging_dirs(self):
        logger.debug("create_staging_dirs")
     
        for d in [ self.config_path, self.seed_path, self.db_path ]:
            cmd = os_commands.check_directory(d, self.sudo, self.uid)
            logger.debug("checking config dir cmd: {}".format(cmd))
            check_dir = execute_bash(source_connection=self.config.connection, command_name=cmd)
            if check_dir.exit_code != 0:
                cmd = os_commands.make_directory(d, self.sudo, self.uid)
                logger.debug("create config dir cmd: {}".format(cmd))
                make_dir = execute_bash(source_connection=self.config.connection, command_name=cmd)
                if make_dir.exit_code != 0:
                    logger.debug("Can't create config directory {}".format(d))
                    raise UserError("Can't create config directory {}".format(d))


    def detach_db(self):
        logger.debug("detach_db")
        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()
        try:
            detach_db = self.run_db_command("detach_database", database=dbname)
        except plugin_exception as p:
            logger.debug("Detach database for {} failed".format(dbname))
            raise UserError("Detach database for {} failed".format(dbname), action="Check output for details", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

    def backup_database(self):
        logger.debug("backup_database")
        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()
        try:
            backup_path=os.path.join(self.seed_path, 'seed_backup_{}'.format(dbname))
            backup_db = self.run_db_command("backup_database", database=dbname, backup_path=backup_path)
        except plugin_exception as p:
            logger.debug("Backup database for {} failed".format(dbname))
            raise UserError("Backup database for {} failed".format(dbname), action="Check output for details", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))


    def restore_seed_database(self):
        logger.debug("restore_seed_database")
        if self.config.dSource:
            dbname = "{}_staging".format(self.get_db_name())
        else:
            dbname = self.get_db_name()
        try:
            backup_path=os.path.join(self.seed_path, 'seed_backup_{}'.format(dbname))
            backup_db = self.run_db_command("restore_seed_database", database=dbname, backup_path=backup_path)
        except plugin_exception as p:
            logger.debug("Restore seed database for {} failed".format(dbname))
            raise UserError("Restore seed database for {} failed".format(dbname), action="Check output for details", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))


    def stop_staging(self):
        """
        if staging is restored from same version of MS SQL, it will be detached on stop
        and file list need to be saved into JSON files
        check for_upgrade what to do
        if backup is from previous - do nothing
        """

        logger.debug("stop_staging")

        if self.config.parameters.for_upgrade:
            logger.debug("for upgrade flag is set - do nothing")
            return

        file_list = self.get_db_file_info()
        filename = os.path.join(self.config_path, "{}_filelist.json".format(self.get_db_name()))
        self.save_json(file_path=filename, obj=file_list)
        self.detach_db()

    def get_file_list_from_file(self):
        logger.debug("get_file_list_from_file")
        filename = os.path.join(self.config_path, "{}_filelist.json".format(self.get_db_name()))
        file_list = self.load_json(filename)
        return file_list

    def switch_to_standby(self):
        logger.debug("switch_to_standby")

        dbname = "{}_staging".format(self.get_db_name())

        try:
            backup_db = self.run_db_command("restore_norecovery_only", database=dbname)
        except plugin_exception as p:
            logger.debug("Restore norecovery stagning database {} failed".format(dbname))
            raise UserError("Restore norecovery stagning database {} failed".format(dbname), action="Check output for details", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))

        try:
            standby_path=os.path.join(self.db_path, 'standby.bak')
            backup_db = self.run_db_command("restore_standby", database=dbname, standby_path=standby_path)
        except plugin_exception as p:
            logger.debug("Switch to standby stagning database {} failed".format(dbname))
            raise UserError("Switch to standbystagning database {} failed".format(dbname), action="Check output for details", output="stdout: {}\nstderr: {}".format(p.stdout, p.stderr))
