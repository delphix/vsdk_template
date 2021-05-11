#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

from controller.helper import execute_bash


def list_db_names(client_path, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} \
        -Q "set nocount on; SELECT name from sys.databases"
        """.format(client_path=client_path, username=username)


def get_backup_file_info(client_path, username, backup_path):
    return """{client_path}/sqlcmd -h -1 -b -U {username} -s '|' <<EOF
                    SET NOCOUNT ON
                    RESTORE FILELISTONLY FROM DISK='{backup_path}'
                    GO
                    EXIT
EOF
    """.format(client_path=client_path, username=username, backup_path=backup_path)


def get_last_backup(connection):
    cmd = """ls -lt --time-style="+%Y-%m-%d %H:%M:%S" /home/delphix/MSSQL_BACKUPS/*"""
    cmd.readlines()
    cmd = execute_bash(source_connection=connection, command_name=cmd)
    new_backups = []
    backup = {}
    for lines in cmd:
        lines = lines.strip()
        lines = lines.splitlines()
        for line in lines:
            line = line.split()
            line = line[5] + ' ' + line[6] + '|' + line[7]
            if line:
                datetime, backup_file = line.split('|')
                backup[datetime] = backup_file
                new_backups.append(backup)
    # print new_backups
    return new_backups


def get_backup_headers(client_path, username, backup_path):
    return """{os_client_path}/sqlcmd -h -1 -b -U {username} -s '|' << EOF
                    SET NOCOUNT ON
                    RESTORE HEADERONLY FROM DISK='{backup_path}'
                    GO
                    EXIT
EOF
        """.format(os_client_path=client_path, username=username, backup_path=backup_path)


def offline_mssql(client_path, database, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    ALTER DATABASE {database} SET offline WITH ROLLBACK IMMEDIATE
                    GO
                    EXIT
EOF
                    """.format(client_path=client_path, database=database, username=username)


def online_mssql(client_path, database, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    ALTER DATABASE {database} SET online
                    GO
            EXIT
EOF
    """.format(client_path=client_path, database=database, username=username)


def drop_database(client_path, database, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    DROP DATABASE {database}
                    GO
                    EXIT
EOF
                    """.format(client_path=client_path, database=database, username=username)


def get_mssql_databases_fileinfo(client_path, database, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} -d {database} -s '|' << EOF
                    SET NOCOUNT ON
                    select f.name, f.file_id, f.physical_name, case f.type when 0 then 'D' when 1 then 'L' end, fg.name as filegroup
                    from sys.database_files f left join sys.filegroups fg on f.data_space_id = fg.data_space_id
                    GO
                    EXIT
EOF
            """.format(client_path=client_path, database=database, username=username)


def list_existing_mssql_databases(client_path, database, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    SET NOCOUNT ON
                    USE MASTER
                    GO
                    SELECT name, database_id, create_date
                    FROM sys.databases
                    GO
                    EXIT
EOF
                    """.format(client_path=client_path, database=database, username=username)


def get_mssql_databases_status(client_path, database, username):
    return """{client_path}/sqlcmd -h -1 -U {username} -s '|' -Q 'set nocount on; SELECT state_desc from sys.databases where name = "{database}" ' """.format(client_path=client_path, database=database, username=username)

# Need update with a loop to create the correct number of files
# and by the {username}me associating theme to the correct filegroup(s)


def create_target_mssql_vdb(client_path, database, username, filename, physical_filename, logname, physical_logname):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    SET NOCOUNT ON
                    USE master
                    GO
                    CREATE DATABASE {database}
                    ON PRIMARY(NAME={filename}, FILENAME='{physical_filename}')
                    LOG ON(NAME={logname}, FILENAME='{physical_logname}')
                    GO
                    EXIT
EOF
                    """.format(client_path=client_path, database=database, username=username, filename=filename, physical_filename=physical_filename,
                               logname=logname, physical_logname=physical_logname)

# Need update with a loop to associate the logical files
# with the correct physical file destination on staging


def restore_full_backup(client_path, database, username, backup_path, mount_path, move, position):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    SET NOCOUNT ON
                    RESTORE DATABASE {database}_staging
                    FROM DISK='{backup_path}'
                    WITH STANDBY='{mount_path}/standby.bak',
                    FILE={position},
                    {move}
                    GO
                    EXIT
EOF
                    """.format(client_path=client_path, database=database, username=username, backup_path=backup_path, mount_path=mount_path, move=move, position=position)


def restore_full_backup_for_upgrade(client_path, database, username, backup_path, mount_path, move, position):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    SET NOCOUNT ON
                    RESTORE DATABASE {database}_staging
                    FROM DISK='{backup_path}'
                    WITH NORECOVERY,
                    FILE={position},
                    {move}
                    GO
                    EXIT
EOF
                    """.format(client_path=client_path, database=database, username=username, backup_path=backup_path, mount_path=mount_path, move=move, position=position)


def restore_transactionlog_backup(client_path, database, username, backup_location, mount_path):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    SET NOCOUNT ON
                    RESTORE LOG  ${database}_staging
                    FROM DISK='{backup_location}'
                    WITH STANDBY='{mount_path}/standby.bak'
                    GO
                    EXIT
EOF
                    """.format(client_path=client_path, database=database, username=username, backup_location=backup_location, mount_path=mount_path)


def get_last_allied_backup(client_path, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    SET NOCOUNT ON
                    SELECT
                    rh.destination_database_name AS[Database],
                    CASE WHEN rh.restore_type='D' THEN 'Database'
                    WHEN rh.restore_type='F' THEN 'File'
                    WHEN rh.restore_type='I' THEN 'Differential'
                    WHEN rh.restore_type='L' THEN 'Log'
                    ELSE rh.restore_type
                    END AS[Restore Type],
                    rh.restore_date AS[Restore Date],
                    bmf.physical_device_name AS[Source],
                    rf.destination_phys_name AS[Restore File],
                    rh.user_name AS[Restored By]
                    FROM msdb.dbo.restorehistory rh
                    INNER JOIN msdb.dbo.backupset bs ON rh.backup_set_id=bs.backup_set_id
                    INNER JOIN msdb.dbo.restorefile rf ON rh.restore_history_id=rf.restore_history_id
                    INNER JOIN msdb.dbo.backupmediafamily bmf ON bmf.media_set_id=bs.media_set_id
                    ORDER BY rh.restore_history_id DESC
                    OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY;
                    GO
                    EXIT
EOF
                    """.format(client_path=client_path, username=username)


# SELECT count(1) groupexist FROM sys.filegroups where name='$fileGroup'

def create_filegroup(client_path, database, username, filegroup):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    ALTER DATABASE {database} ADD FILEGROUP {filegroup}
                    GO
            EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, filegroup=filegroup)


def add_datafile_to_group(client_path, database, username, filegroup, filename, physical_filename):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    ALTER DATABASE {database} ADD FILE
                    (name = "{filename}" , filename = "{physical_filename}") TO FILEGROUP {filegroup}
                    GO
            EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, filegroup=filegroup, filename=filename, physical_filename=physical_filename)


def add_datafile(client_path, database, username, filename, physical_filename):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    ALTER DATABASE {database} ADD FILE
                    (name = "{filename}" , filename = "{physical_filename}")
                    GO
            EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, filename=filename, physical_filename=physical_filename)


def add_logfile(client_path, database, username, logfile, physical_logfile):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    ALTER DATABASE {database} ADD LOG FILE
                    (name = "{logfile}" , filename = "{physical_logfile}")
                    GO
            EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, logfile=logfile, physical_logfile=physical_logfile)


def drop_datafile(client_path, database, username, filename):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    ALTER DATABASE {database} REMOVE FILE {filename}
                    GO
            EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, filename=filename)


def shrink_datafile(client_path, database, username, filename):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    USE {database}
                    GO
                    DBCC SHRINKFILE ('{filename}', EMPTYFILE)
                    GO
            EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, filename=filename)


def rename_datafile(client_path, database, username, filename, physical_filename):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
                    ALTER DATABASE {database} MODIFY FILE
                    (name = "{filename}" , filename = "{physical_filename}")
                    GO
            EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, filename=filename, physical_filename=physical_filename)

# ALTER DATABASE [$databaseName] ADD FILE (name = N'$logicalName', filename = N'$physicalPath') TO FILEGROUP [$fileGroup]

# ALTER DATABASE [$databaseName] ADD LOG FILE (name = N'$logicalName', filename = N'$physicalPath')


def get_last_backup_infos(os_client_path, database):
    return """{os_client_path}/sqlcmd -h -1 -b -S {hostname} -U SA << EOF
        SET NOCOUNT ON
        SELECT
        rh.restore_date AS[Restore Date],
        bmf.physical_device_name AS[Source]
        FROM msdb.dbo.restorehistory rh
        INNER JOIN msdb.dbo.restorefile rf ON rh.restore_history_id=rf.restore_history_id
        INNER JOIN msdb.dbo.backupset bs ON rh.backup_set_id=bs.backup_set_id
                        INNER JOIN msdb.dbo.backupmediafamily bmf ON bmf.media_set_id=bs.media_set_id
        ORDER BY rh.restore_history_id DESC
        OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY;
        GO
        EXIT
EOF
        """.format(os_client_path=os_client_path, database=database)


def attach_database(client_path, database, username, files):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
    CREATE DATABASE {database} ON 
    {files}
    FOR ATTACH
    GO
    EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, files=files)

def detach_database(client_path, database, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
    exec sp_detach_db '{database}', True
    GO
    EXIT
EOF
    """.format(client_path=client_path, database=database, username=username)


def backup_database(client_path, database, username, backup_path):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
    backup database {database} to disk='{backup_path}'
    GO
    EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, backup_path=backup_path)

def restore_seed_database(client_path, database, username, backup_path):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
    restore database {database} from disk='{backup_path}' WITH NORECOVERY, REPLACE
    GO
    EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, backup_path=backup_path)


def restore_norecovery_only(client_path, database, username):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
    restore database {database} WITH NORECOVERY
    GO
    EXIT
EOF
    """.format(client_path=client_path, database=database, username=username)


def restore_standby(client_path, database, username, standby_path):
    return """{client_path}/sqlcmd -h -1 -b -U {username} << EOF
    restore database {database} WITH standby='{standby_path}'
    GO
    EXIT
EOF
    """.format(client_path=client_path, database=database, username=username, standby_path=standby_path)
