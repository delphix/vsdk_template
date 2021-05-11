#
# Copyright (c) 2020 by Delphix. All rights reserved.
#



class plugin_exception(Exception):
    """
    Class to define a plugin exception based on the OS command response
    """
    def __init__(self, os_command_response):
        self.__stdout = os_command_response.stdout
        self.__stderr = os_command_response.stderr
        self.__exit_code = os_command_response.exit_code


    @property
    def stdout(self):
        return self.__stdout

    @property
    def stderr(self):
        return self.__stderr

    @property
    def exit_code(self):
        return self.__exit_code

