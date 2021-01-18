import json
import argparse
import os
import sys
import time
from datetime import datetime
from nfs_to_s3 import NFSs3

def nfs_file_existence_check():

    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('-source_dir', type=str, required=True)
    my_parser.add_argument('-file_pattern', type=str, required=True)
    my_parser.add_argument('-file_count', type=int, default=None)
    my_parser.add_argument('-poke_interval', type=int, default=1)
    my_parser.add_argument('-no_of_retries', type=int, default=0)
    args = my_parser.parse_args()
    nfss3 = NFSs3()
    retry_count = 0
    file_exists = False
    while not file_exists and retry_count <= args.no_of_retries:
        if retry_count > 0:
            if retry_count == 1:
                suffix = 'st'
            elif retry_count == 2:
                suffix = 'nd'
            elif retry_count == 3:
                suffix = 'rd'
            else:
                suffix = 'th'

            print("Retrying {retry_count}{suffix} time of total {no_of_retries} retries in {poke_interval} seconds".format(retry_count=retry_count, suffix=suffix, no_of_retries=args.no_of_retries, poke_interval=args.poke_interval))
            time.sleep(args.poke_interval)

        file_exists = nfss3.is_file_present(args.source_dir, args.file_pattern, args.file_count)
        retry_count+=1

    sys.exit(0 if file_exists else 1)
if __name__ == "__main__":
    nfs_file_existence_check()
