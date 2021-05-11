#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging


logger = logging.getLogger(__name__)



class config_meta(object):
    def __init__(self,
                 source_connection=None,
                 repository=None,
                 source_config=None,
                 snapshot_parameters=None,
                 staged_source=None,
                 virtual_source=None,
                 snapshot=None):

        """
        Configuration object - to keep all different objects provided by vSDK objects
        :param source_connection: Source connection object
        :param repository: instance of Repository class
        :param source_config: instance of SourceConfig
        :param snapshot_parameters: object of snapshot definition
        :param staged_source: object of staged source
        :param virtual_source: object of virtual source
        :param snapshot: snapshot object created at dsource time

        Depend on the init arguments - dSource flag is set to True or False
        connection parameter is always set to a connection provided by vSDK
        source parameter is pointed to staged_source or virtual source - depend on init parameters
        """

        self.source_connection = source_connection
        self.repository = repository
        self.source_config = source_config
        self.staged_source = staged_source
        self.snapshot_parameters = snapshot_parameters
        self.virtual_source = virtual_source
        self.snapshot = snapshot

        # Entry for other parameters which is being used in function

        if virtual_source is None:
            self.dSource = True
            self.connection = staged_source.staged_connection
            self.source = staged_source
        else:
            self.dSource = False
            self.connection = virtual_source.connection
            self.source = virtual_source
            

        self.parameters = self.source.parameters


