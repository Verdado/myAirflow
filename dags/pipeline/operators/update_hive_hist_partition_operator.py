import copy
from urllib.parse import urlparse

from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hdfs.util import HdfsError
from hmsclient.genthrift.hive_metastore.ttypes import Partition, AddPartitionsRequest


class UpdateHiveHistPartitionOperator(BaseOperator):
    """
    Operator for updating partition locations of a historical table (SCD) to the latest version.
    Example:
      path/to/hist/table/active=true/start_year=2019/start_month=12/spdb_ver=1 <- Drop this partition
                                    /start_year=2020/start_month=01/spdb_ver=1
                                                                   /spdb_ver=2 <- Point to this new partition location
                                                    /start_month=02/spdb_ver=2 <- Add new partition
                       /active=false/start_year=2019/start_month=12/spdb_ver=2 <- Add new partition
                                     start_year=2020/start_month=01/spdb_ver=1
                                                                   /spdb_ver=2 <- Point to this new partition location

    Note: Some changes need to be done before using this operator for big tables with partitioning columns of their own
    """

    @apply_defaults
    def __init__(
            self,
            database,
            table,
            active_flag='spdb_active',
            partition_version_key='spdb_ver',
            delete_old_versions=True,
            versions_to_retain=2,
            metastore_conn_id='metastore_default',
            webhdfs_conn_id='webhdfs_default',
            *args, **kwargs):
        assert versions_to_retain > 1, "At least 2 partition versions should be retained to ensure table availability"

        super(UpdateHiveHistPartitionOperator, self).__init__(*args, **kwargs)
        self.database = database
        self.table = table
        self.active_flag = active_flag
        self.partition_version_key = partition_version_key
        self.delete_old_versions = delete_old_versions
        self.versions_to_retain = versions_to_retain
        self.metastore_conn_id = metastore_conn_id
        self.webhdfs_conn_id = webhdfs_conn_id

        self.hdfs_conn = None

    def _get_all_partition_paths(self, table_path, partition_keys):
        if not self.hdfs_conn.status(table_path, strict=False):
            return []
        dir_infos = self.hdfs_conn.walk(table_path, depth=len(partition_keys))
        return [path + "/" + d for path, dirs, _ in dir_infos for d in dirs if d.startswith(partition_keys[-1] + '=')]

    def _get_partition_version_dirs(self, partition_path):
        try:
            versions = [version_dir for version_dir in self.hdfs_conn.list(partition_path)
                        if version_dir.startswith(f"{self.partition_version_key}=")]
            versions.sort()
        except HdfsError:
            versions = []
            self.log.info(f"{partition_path} does not exist")
        return versions

    def _get_partition_version_paths(self, partition_path):
        return [partition_path + '/' + version_dir for version_dir in self._get_partition_version_dirs(partition_path)]

    def _get_partition_values(self, path, partition_keys):
        partitions = dict(partition_dir.split('=') for partition_dir in path.split('/') if '=' in partition_dir)
        return [partitions.get(k) for k in partition_keys]

    def _get_partition_name(self, path, partition_keys):
        return "/".join([dir for dir in path.split('/') if '=' in dir and dir.split("=")[0] in partition_keys])

    def _make_partition(self, ms_table, partition_path, partition_values):
        sd = copy.deepcopy(ms_table.sd)
        sd.location = 'hdfs://nameservice1' + partition_path
        return Partition(values=partition_values, dbName=self.database, tableName=self.table, sd=sd)

    def execute(self, context):
        self.hdfs_conn = WebHDFSHook(self.webhdfs_conn_id).get_conn()
        metastore_hook = HiveMetastoreHook(self.metastore_conn_id)

        # Get current table metadata
        ms_table = metastore_hook.get_table(self.table, self.database)
        table_path = ms_table.sd.location
        table_path = urlparse(table_path).path if table_path.startswith("hdfs://") else table_path
        partition_keys = [k.name for k in ms_table.partitionKeys]
        self.log.info(f"Table location: {table_path} \n, partition keys: {partition_keys}")
        assert partition_keys[0] == self.active_flag, "1st partition key must be active flag for this operator to work"

        new_partitions = []
        partitions_to_drop = []
        paths_to_delete = []

        # Active partitions: Populate partitions to add, drop or update and version to delete
        # For active partitions, only partitions with the latest version path should be retained
        active_partition_paths = self._get_all_partition_paths(
            f"{table_path}/{self.active_flag}=true", partition_keys[1:])
        latest_version = ''
        version_dirs_dict = {}
        for active_partition_path in active_partition_paths:
            version_dirs = self._get_partition_version_dirs(active_partition_path)
            if version_dirs:
                latest_version = max(version_dirs[-1], latest_version)
                version_dirs_dict[active_partition_path] = version_dirs
        self.log.info(f"Latest active version: {latest_version}")
        for active_partition_path, version_dirs in version_dirs_dict.items():
            if latest_version in version_dirs:
                latest_version_path = f"{active_partition_path}/{latest_version}"
                partition_values = self._get_partition_values(latest_version_path, partition_keys)
                new_partition = self._make_partition(ms_table, latest_version_path, partition_values)
                new_partitions.append(new_partition)
                if self.delete_old_versions:
                    # Populate partition versions to delete
                    paths_to_delete += [f"{active_partition_path}/{v}" for v in version_dirs[:-self.versions_to_retain]]
            else:
                partition_name = self._get_partition_name(active_partition_path, partition_keys)
                partitions_to_drop.append(partition_name)
                if self.delete_old_versions:
                    paths_to_delete.append(active_partition_path)

        # Inactive partitions: Populate partitions to add or update and versions to delete
        inactive_partition_paths = self._get_all_partition_paths(
            f"{table_path}/{self.active_flag}=false", partition_keys[1:])
        for inactive_partition_path in inactive_partition_paths:
            version_dirs = self._get_partition_version_paths(inactive_partition_path)
            if version_dirs:
                latest_version_path = version_dirs[-1]
                self.log.info(f"Latest version path: {latest_version_path}")
                partition_values = self._get_partition_values(latest_version_path, partition_keys)
                new_partition = self._make_partition(ms_table, latest_version_path, partition_values)
                new_partitions.append(new_partition)
                if self.delete_old_versions:
                    # Populate partition versions to delete
                    paths_to_delete += version_dirs[:-self.versions_to_retain]

        # Add new partitions, drop old partitions and update existing partitions
        with metastore_hook.get_conn() as metastore_conn:
            req = AddPartitionsRequest(
                dbName=self.database, tblName=self.table, parts=new_partitions, ifNotExists=True, needResult=True)
            response = metastore_conn.add_partitions_req(req)
            self.log.info(f"Added new partitions: {response}")

            response = metastore_conn.drop_partitions(
                db_name=self.database, table_name=self.table, names=partitions_to_drop, need_result=True)
            self.log.info(f"Dropped active partitions that no longer exist: {response}")

            metastore_conn.alter_partitions(self.database, self.table, new_partitions)
            self.log.info(f"Altered location of existing partitions to latest version")

        # Delete old partition versions and dropped active partitions
        if self.delete_old_versions:
            for path in paths_to_delete:
                self.log.info(f"Removing old version path: {path}")
                res = self.hdfs_conn.delete(path, recursive=True)
                if not res:
                    self.log.error(f"Could not remove data at {path}")
            self.log.info(f"Removed old partition versions and dropped active partitions")
