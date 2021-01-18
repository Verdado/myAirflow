from hdfs.util import HdfsError
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MoveProcessedIncrPartitionsOperator(BaseOperator):
    template_fields = ['last_partition_dir']

    @apply_defaults
    def __init__(
            self,
            origin_path,
            destination_path,
            partition_key,
            last_partition,
            skip_flag_file=None,
            webhdfs_conn_id='webhdfs_default',
            *args, **kwargs):
        super(MoveProcessedIncrPartitionsOperator, self).__init__(*args, **kwargs)
        self.origin_path = origin_path
        self.destination_path = destination_path
        self.partition_key = partition_key
        self.last_partition_dir = partition_key + '=' + last_partition
        self.skip_flag_file = skip_flag_file

        self.webhdfs_conn_id = webhdfs_conn_id
        self.hdfs_conn = None

    def _get_dirs_to_move(self):
        try:
            dirs_to_move = [partition_dir for partition_dir in self.hdfs_conn.list(self.origin_path)
                            if partition_dir.startswith(self.partition_key + '=')
                            and partition_dir < self.last_partition_dir]
        except HdfsError:
            self.log.warn("The origin path, {dir} doesn't exist.".format(dir=self.origin_path))
            return []
        return dirs_to_move

    def execute(self, context):
        self.hdfs_conn = WebHDFSHook(webhdfs_conn_id=self.webhdfs_conn_id).get_conn()

        # If skip flag is present under origin path, then skip moving
        if self.skip_flag_file is not None and self.hdfs_conn.status(
                '{path}/{file}'.format(path=self.origin_path, file=self.skip_flag_file), strict=False) is not None:
            self.log.info('Skipping moving partitions, as {flag_file} is present'.format(flag_file=self.skip_flag_file))
            return

        self.log.info('Moving partitions of incremental files before {last_dir} from {origin} to {destination}'.format(
            last_dir=self.last_partition_dir, origin=self.origin_path, destination=self.destination_path))

        self.hdfs_conn.makedirs(self.destination_path)
        dirs_in_destination_path = set(self.hdfs_conn.list(self.destination_path))
        for target_dir in self._get_dirs_to_move():
            self.log.info('Moving {dir}'.format(dir=target_dir))
            if target_dir in dirs_in_destination_path:
                self.log.info(f'{target_dir} already exists at {self.destination_path}. Will merge with that dir')
                for file_to_move in self.hdfs_conn.list(f'{self.origin_path}/{target_dir}'):
                    self.hdfs_conn.rename(f'{self.origin_path}/{target_dir}/{file_to_move}',
                                          f'{self.destination_path}/{target_dir}')
                self.hdfs_conn.delete(f'{self.origin_path}/{target_dir}')
            else:
                self.hdfs_conn.rename(f'{self.origin_path}/{target_dir}', self.destination_path)

        self.log.info("Moving completed")
