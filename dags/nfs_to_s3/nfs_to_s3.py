import libnfs
from datetime import datetime
from re import search
import time
import os
import s3fs

class NFSs3:
    def __init__(self, nfs_host_name=None, key=None, secret=None, endpoint_url=None):
        self.nfs_host_name = nfs_host_name if nfs_host_name else os.environ["NFS_HOST_NAME"]
        self.key = key if key else os.environ["KEY"]
        self.secret = secret if secret else os.environ["SECRET"]
        self.endpoint_url = endpoint_url if endpoint_url else os.environ["ENDPOINT_URL"]
        self.s3fs_conn = s3fs.S3FileSystem(anon=False, key=self.key, secret=self.secret, client_kwargs={'endpoint_url':self.endpoint_url})

    def copy_file_nfs_to_s3(self, source_dir, file_pattern, target_bucket):
        nfs = libnfs.NFS('nfs://' + self.nfs_host_name + source_dir)
        files_list = list(filter(lambda v: search(file_pattern, v), nfs.listdir('.')))
        if files_list:
            for file_name in files_list:
                print("Copying file '{file_name}' from '{nfs_host_name}{source_dir}' to '{target_bucket}'".format(file_name=file_name, nfs_host_name=self.nfs_host_name, source_dir=source_dir, target_bucket=target_bucket))
                data = libnfs.open('nfs://' + self.nfs_host_name + source_dir + '/' + file_name, mode='rb')
                with self.s3fs_conn.open( target_bucket + '/' + file_name, 'wb') as f:
                    f.write(data.read())
            return 0
        else:
            print("No files found with the following pattern: '{file_pattern}'".format(file_pattern=file_pattern))
            return 1

    def is_file_present(self, source_dir, file_pattern, file_count=None):
        nfs = libnfs.NFS('nfs://' + self.nfs_host_name + source_dir)
        files_list = list(filter(lambda v: search(file_pattern, v), nfs.listdir('.')))
        if files_list:
            if file_count:
               if len(files_list) == file_count:
                   print("Expected number '{file_count}' of files found in '{nfs_host_name}{source_dir}' directory with file pattern '{file_pattern}'".format(file_count=file_count, nfs_host_name=self.nfs_host_name, source_dir=source_dir, file_pattern=file_pattern))
                   print('List of files found: {files_list}'.format(files_list=files_list))
                   return True
               else:
                   print("{actual_file_count} files are present in '{nfs_host_name}{source_dir}' directory with file pattern '{file_pattern}'. Expected file count is {file_count}".format(actual_file_count=len(files_list), nfs_host_name=self.nfs_host_name, source_dir=source_dir, file_pattern=file_pattern, file_count=file_count))
                   return False
            else:
                print("'{actual_file_count}' files found in '{nfs_host_name}{source_dir}' directory with file pattern '{file_pattern}'".format(actual_file_count=len(files_list), source_dir=source_dir, nfs_host_name=self.nfs_host_name, file_pattern=file_pattern))
                print('List of files found: {files_list}'.format(files_list=files_list))
                return True
        else:
            print("No file found in '{nfs_host_name}{source_dir}' directory with file pattern '{file_pattern}'".format(nfs_host_name=self.nfs_host_name, source_dir=source_dir, file_pattern=file_pattern))
            return False

