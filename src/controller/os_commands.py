#
# Copyright (c) 2020 by Delphix. All rights reserved.
#
# Module with list of OS commands


def check_backup_dir(backup_dir):
    return ("""ls -lt --time-style="+%Y-%m-%d %H:%M:%S" {backup_dir}/*""").readlines()


def echo_var(variable):
    return "echo ${variable}".format(variable=variable)


def find_file(path, filename, sudo=False, uid=None):
    if sudo:
        return "sudo -u \#{uid} find {path} -name {filename}".format(path=path, filename=filename, uid=uid)
    else:
        return "find {path} -name {file}".format(path=path, file=file)


def get_process():
    return "ps -ef"


def make_directory(directory_path, sudo=False, uid=None):
    if sudo:
        return "sudo -u \#{uid} mkdir -p {directory_path}".format(uid=uid, directory_path=directory_path)
    else:
        return "mkdir -p {directory_path}".format(directory_path=directory_path)


def change_permission(path, sudo=False, uid=None):
    if sudo:
        return "sudo -u \#{uid} chmod -R 775 {path}".format(uid=uid, path=path)
    else:
        return "chmod -R 775 {path}".format(path=path)


def read_file(filename, sudo=False, uid=None):
    if sudo:
        return "sudo -u \#{uid} cat {filename}".format(filename=filename, uid=uid)
    else:
        return "cat {filename}".format(filename=filename)


def check_file(file_path, sudo=False, uid=None):
    if sudo:
        return "sudo -u \#{uid} [ -f {file_path} ]".format(file_path=file_path, uid=uid)
    else:
        return "[ -f {file_path} ]".format(file_path=file_path)


def check_directory(dir_path, sudo=False, uid=None):
    if sudo:
        return "sudo -u \#{uid} [ -d {dir_path} ]".format(dir_path=dir_path, uid=uid)
    else:
        return "[ -d {dir_path} ]".format(dir_path=dir_path)


def write_file(filename, data, sudo=False, uid=None):
    if sudo:
        return "echo {data} | sudo -u \#{uid} tee {filename} > /dev/null ".format(filename=filename, data=data, uid=uid)
    else:
        return "echo {data} | tee {filename} > /dev/null".format(filename=filename, data=data)


def get_ip_of_hostname():
    return "hostname -i"


def get_ids(install_path):
    return "ls -n {install_path}".format(install_path=install_path)


def get_hostname(short=True):
    if short:
        return "hostname -s"
    else:
        return "hostname -f"


def delete_file(filename, force=False, sudo=False, uid=None):
    if force:
        fopt = "-f"
    else:
        fopt = ""
    if sudo:
        return "sudo -u \#{uid} rm {fopt} {filename}".format(filename=filename, uid=uid, fopt=fopt)
    else:
        return "rm {fopt} {filename}".format(filename=filename, fopt=fopt)


def delete_dir(dir_path, force=False, sudo=False, uid=None):
    if force:
        fopt = "-f"
    else:
        fopt = ""
    if sudo:
        return "sudo -u \#{uid} rm -r {fopt} {dir_path}".format(dir_path=dir_path, uid=uid, fopt=fopt)
    else:
        return "rm -r {fopt} {dir_path}".format(dir_path=dir_path, fopt=fopt)


def os_mv(srcname, trgname, sudo=False, uid=None):
    if sudo:
        return "sudo -u \#{uid} mv {srcname} {trgname}".format(srcname=srcname, trgname=trgname, uid=uid)
    else:
        return "mv {srcname} {trgname}".format(srcname=srcname, trgname=trgname)


def os_cp(srcname, trgname, sudo=False, uid=None):
    if sudo:
        return "sudo -u \#{uid} cp {srcname} {trgname}".format(srcname=srcname, trgname=trgname, uid=uid)
    else:
        return "cp {srcname} {trgname}".format(srcname=srcname, trgname=trgname, uid=uid)


def whoami():
    return "id"


def sed(filename, regex):
    return 'sed -i -e "{}" {}'.format(regex, filename)


def rpm(package_name):
    return "rpm -q {package_name}".format(package_name=package_name)


def list_dir(dir_path, sudo=False, uid=None):
    if sudo:
        return """sudo -u \#{uid} ls -l --time-style="+%Y-%m-%d %H:%M:%S" {dir_path}""".format(dir_path=dir_path, uid=uid)
    else:
        return """ls -l --time-style="+%Y-%m-%d %H:%M:%S" {dir_path}""".format(dir_path=dir_path)