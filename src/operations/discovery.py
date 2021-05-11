#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging
import sys
import traceback

from dlpx.virtualization.platform.exceptions import UserError
from mssql.mssql_discovery import return_repository

logger = logging.getLogger(__name__)

def find_repos(source_connection):
    """
    Run a discovery on server to find a repository
    Returns:
        Object of RepositoryDefinition class
    """

    try:

        return return_repository(source_connection)
    
    except UserError:
        # pass lower code exception
        ttype, value, tracebk = sys.exc_info()
        logger.debug("General exception handing in find_repos")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(tracebk.format_exc())
        raise

    except Exception:
        ttype, value, tracebk = sys.exc_info()
        logger.debug("General exception handing in find_repos")
        logger.debug("type: {}, value: {}".format(ttype, value))
        logger.debug("trackback")
        logger.debug(tracebk.format_exc())
        UserError(message="Unhandled exception. Please contact Delphix", output=traceback.format_exc())

