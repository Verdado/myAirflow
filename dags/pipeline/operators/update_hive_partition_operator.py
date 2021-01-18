import copy
from urllib.parse import urlparse

from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hdfs.util import HdfsError
from hmsclient.genthrift.hive_metastore.ttypes import Partition, AddPartitionsRequest


class UpdateHivePartitionOperator(BaseOperator):
    """
    Operator for updating partition locations of a hive table to the latest version.
    Example:
      path/to/table/part1=a/part2=x/spdb_ver=1
      path/to/table/part1=a/part2=x/spdb_ver=2 <- Partition metadata will be updated to point to this location

    This operator doesn't handle schema change and may not work well with tables that get hard deletes.
    """

    @apply_defaults
    def __init__(
            self,
            database,
            table,
            delete_old_versions=True,
            versions_to_retain=2,
            metastore_conn_id='metastore_default',
            webhdfs_conn_id='webhdfs_default',
            *args, **kwargs):
        assert versions_to_retain > 1, "At least 2 partition versions should be retained to ensure table availability"

        super(UpdateHivePartitionOperator, self).__init__(*args, **kwargs)
        self.database = database
        self.table = table
        self.delete_old_versions = delete_old_versions
        self.versions_to_retain = versions_to_retain
        self.metastore_conn_id = metastore_conn_id
        self.webhdfs_conn_id = webhdfs_conn_id

        self.hdfs_conn = None

    def _get_all_partition_paths(self, table_path, partition_keys):
        dir_infos = self.hdfs_conn.walk(table_path, depth=len(partition_keys))
        return [path + "/" + d for path, dirs, _ in dir_infos for d in dirs if d.startswith(partition_keys[-1] + '=')]

    def _get_partition_version_paths(self, partition_path):
        try:
            versions = self.hdfs_conn.list(partition_path)
            versions.sort()
        except HdfsError:
            versions = []
            self.log.info(f"{partition_path} does not exist")
        return [partition_path + '/' + version for version in versions]

    def _get_partition_values(self, path, partition_keys):
        partitions = dict(partition_dir.split('=') for partition_dir in path.split('/') if '=' in partition_dir)
        return [partitions.get(k) for k in partition_keys]

    def _make_partition(self, ms_table, partition_path, partition_values):
        sd = copy.deepcopy(ms_table.sd)
        sd.location = 'hdfs://nameservice1' + partition_path
        return Partition(values=partition_values, dbName=self.database, tableName=self.table, sd=sd)

    def _make_new_table(self, ms_table, latest_version_path):
        new_ms_table = copy.deepcopy(ms_table)
        new_ms_table.sd.location = 'hdfs://nameservice1' + latest_version_path
        # remove SERDEPROPERTIES.path to avoid double reads
        if 'path' in new_ms_table.sd.serdeInfo.parameters:
            del new_ms_table.sd.serdeInfo.parameters['path']
        return new_ms_table

    def execute(self, context):
        self.hdfs_conn = WebHDFSHook(self.webhdfs_conn_id).get_conn()
        metastore_hook = HiveMetastoreHook(self.metastore_conn_id)

        # Get current table metadata
        ms_table = metastore_hook.get_table(self.table, self.database)
        table_path = ms_table.sd.location.rstrip('/')
        table_path = urlparse(table_path).path if table_path.startswith("hdfs://") else table_path
        partition_keys = [k.name for k in ms_table.partitionKeys]
        self.log.info(f"Table location: {table_path} \n, partition keys: {partition_keys}")

        version_paths_to_delete = []

        # For non-partitioned table
        if not partition_keys:
            self.log.info("Table is not partitioned, so updating the table location to latest version")
            table_path = table_path.rsplit('/', 1)[0] if "=" in table_path.rsplit('/', 1)[1] else table_path
            version_paths = self._get_partition_version_paths(table_path)
            if version_paths:
                latest_version_path = version_paths[-1]
                self.log.info(f"Latest version path: {latest_version_path}")
                new_ms_table = self._make_new_table(ms_table, latest_version_path)
                with metastore_hook.get_conn() as metastore_conn:
                    metastore_conn.alter_table(self.database, self.table, new_ms_table)
                if self.delete_old_versions:
                    version_paths_to_delete += version_paths[:-self.versions_to_retain]

        # For partitioned table
        else:
            self.log.info("Table is partitioned, so updating location of partitions to latest version")
            # Get all partition paths from HDFS
            partition_paths = self._get_all_partition_paths(table_path, partition_keys)

            # Populate partitions to add or update
            new_partitions = []
            for partition_path in partition_paths:
                version_paths = self._get_partition_version_paths(partition_path)
                if version_paths:
                    latest_version_path = version_paths[-1]
                    self.log.info(f"Latest version path: {latest_version_path}")
                    partition_values = self._get_partition_values(latest_version_path, partition_keys)
                    new_partition = self._make_partition(ms_table, latest_version_path, partition_values)
                    new_partitions.append(new_partition)

                    if self.delete_old_versions:
                        # Populate partition versions to delete
                        version_paths_to_delete += version_paths[:-self.versions_to_retain]

            # Add new partitions and update existing partitions
            with metastore_hook.get_conn() as metastore_conn:
                req = AddPartitionsRequest(
                    dbName=self.database, tblName=self.table, parts=new_partitions, ifNotExists=True, needResult=True)
                response = metastore_conn.add_partitions_req(req)
                self.log.info(f"Added new partitions: {response}")

                metastore_conn.alter_partitions(self.database, self.table, new_partitions)
                self.log.info(f"Altered location of existing partitions to latest version")

        # Delete old partition versions
        if self.delete_old_versions:
            for path in version_paths_to_delete:
                self.log.info(f"Removing old version path: {path}")
                res = self.hdfs_conn.delete(path, recursive=True)
                if not res:
                    self.log.error(f"Could not remove data at {path}")
            self.log.info(f"Removed old partition versions")
