'''
This function converts a folder of protobuf formated GTFS-Realtime Trip Updates file into csv file.
Each folder suggest to contain one day GTFS data.
This code use the multiprocessing function for a faster processing.

One input required: The path to the protobuf folder.
The path should be inputed after calling the file name 'tu_pb_to_csv_folder.py',
as the second System Specific Parameter (sys.argv[1])
'''

# import packages
import sys
import tu_pb_to_csv
import multiprocessing
from datetime import datetime
import glob
import os


# Get protobuf folder directory
dir_ori = str(sys.argv[1])
# dir_ori = '/Volumes/research-data/PRJ-SCALUT/GTFS_tim/10_raw_pb_tu/test/'

# Check whether a file is imported instead of a folder.
# Make sure the input dir_ori is only for a protobuf folder.
if dir_ori[-6:] == '.pb.gz' or dir_ori[-3:] == '.pb':
    print('A folder path is required as the first input, not a file path')
    sys.exit()

# Add '/' to the folder of the path has no '/' at the end
if dir_ori[-1:] != '/':
    dir_ori += '/'

# Define the function for multiprocessing
def pd_to_csv(src):
    tu_pb_to_csv.tu_pb_to_csv(src,'')

# Start the parallel processing
if __name__ == '__main__':

    # Get start time
    tStart = datetime.now()
    # Check number of CPUs
    np = multiprocessing.cpu_count()
    # Generate process list
    processes = []
    # Set up pool by the number of CPUs
    pool = multiprocessing.Pool(processes = np)
    # Get file paths within the protobuf folder
    ls_dir = glob.glob(os.path.join(dir_ori + '*.pb.gz'))

    # Raise an error if no file found
    if len(ls_dir) == 0:
        print('No File Found')
        sys.exit()

    # Run the funtion using multiprocessing
    result = pool.map(pd_to_csv, ls_dir)

    # Print process time
    print('That took {} minutes'.format((datetime.now() - tStart)/60))

    # Exit
    sys.exit()
