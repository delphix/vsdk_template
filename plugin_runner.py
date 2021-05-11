import logging

from dlpx.virtualization.platform import Mount, MountSpecification, Plugin

from generated.definitions import (
    RepositoryDefinition,
    SourceConfigDefinition,
    SnapshotDefinition,
)

from operations.discovery import find_repos
from controller.helper import setup_logger
from operations import linked
from operations import virtual

plugin = Plugin()
setup_logger()
logger = logging.getLogger(__name__)

#
# Below is an example of the repository discovery operation.
#
# NOTE: The decorators are defined on the 'plugin' object created above.
#
# Mark the function below as the operation that does repository discovery.


@plugin.discovery.repository()
def repository_discovery(source_connection):
    #
    # This is an object generated from the repositoryDefinition schema.
    # In order to use it locally you must run the 'build -g' command provided
    # by the SDK tools from the plugin's root directory.
    #

    return find_repos(source_connection)


@plugin.discovery.source_config()
def source_config_discovery(source_connection, repository):
    #
    # To have automatic discovery of source configs, return a list of
    # SourceConfigDefinitions similar to the list of
    # RepositoryDefinitions above.
    #

    return []


@plugin.linked.pre_snapshot()
def linked_pre_snapshot(staged_source, repository, source_config, snapshot_parameters):
    if int(snapshot_parameters.resync) == 1:
        linked.resync(staged_source, repository, source_config)
    else:
        linked.pre_snapshot(staged_source, repository, source_config)


@plugin.linked.post_snapshot()
def linked_post_snapshot(staged_source,
                         repository,
                         source_config,
                         snapshot_parameters):
    return linked.post_snapshot(staged_source, repository, source_config)


@plugin.linked.mount_specification()
def linked_mount_specification(staged_source, repository):
    return linked.staging_mount_point(staged_source, repository)


@plugin.linked.start_staging()
def linked_start_staging(staged_source, repository, source_config):
    linked.start_staging(staged_source, repository, source_config)


@plugin.linked.stop_staging()
def linked_stop_staging(staged_source, repository, source_config):
    linked.stop_staging(staged_source, repository, source_config)


@plugin.linked.status()
def linked_status(staged_source, repository, source_config):
    return linked.staging_status(staged_source, repository, source_config)

@plugin.virtual.configure()
def configure(virtual_source, snapshot, repository):
    return virtual.configure(virtual_source, snapshot, repository)


@plugin.virtual.unconfigure()
def unconfigure(virtual_source, repository, source_config):
    return virtual.unconfigure(virtual_source, repository, source_config)


@plugin.virtual.reconfigure()
def reconfigure(virtual_source, repository, source_config, snapshot):
    return virtual.reconfigure(virtual_source, repository, source_config, snapshot)


@plugin.virtual.post_snapshot()
def virtual_post_snapshot(virtual_source, repository, source_config):
    return virtual.post_snapshot(virtual_source, repository, source_config)


@plugin.virtual.mount_specification()
def virtual_mount_specification(virtual_source, repository):
    return virtual.virtual_mount_specification(virtual_source, repository)


@plugin.virtual.status()
def virtual_status(virtual_source, repository, source_config):
    return virtual.vdb_status(virtual_source, repository, source_config)


@plugin.virtual.start()
def start(virtual_source, repository, source_config):
    virtual.start_vdb(virtual_source, repository, source_config)


@plugin.virtual.stop()
def stop(virtual_source, repository, source_config):
    virtual.stop_vdb(virtual_source, repository, source_config)
