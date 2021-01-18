import argparse
import sys
from nfs_to_s3 import NFSs3

def main():

    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('-source_dir', type=str, required=True)
    my_parser.add_argument('-file_pattern', type=str, required=True)
    my_parser.add_argument('-target_bucket', type=str, required=True)

    args = my_parser.parse_args()

    nfss3 = NFSs3()
    status = nfss3.copy_file_nfs_to_s3(args.source_dir, args.file_pattern, args.target_bucket)
    sys.exit(status)
if __name__ == "__main__":
    main()

