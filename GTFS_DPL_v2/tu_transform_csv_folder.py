'''
This function cleans a folder of csv files by renaming columns and removing unreasonable results.
Each folder suggest to contain one day GTFS data.
This code use the multiprocessing function for a faster processing.

One input required: The path to the csv folder.
The path should be inputed after calling the file name 'tu_transform_csv_folder.py',
as the second System Specific Parameter (sys.argv[1])
'''

# import packages
import tu_transform_csv
import sys
import multiprocessing
from datetime import datetime
import glob
import os

# Get csv folder directory
dir_ori = str(sys.argv[1])

# Check whether a file is imported instead of a folder.
# Make sure the input dir_ori is only for a csv folder.
if dir_ori[-7:] == '.csv.gz' or dir_ori[-4:] == '.csv':
    print('A folder path is required as the first input, not a file path')
    sys.exit()

# Add '/' to the folder of the path has no '/' at the end
if dir_ori[-1:] != '/':
    dir_ori += '/'

# Define the function for multiprocessing
def transform_csv(src):
    tu_transform_csv.tu_transform_csv(src,'')

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
    # Get file paths within the csv folder
    ls_dir = glob.glob(os.path.join(dir_ori + '*.csv.gz'))

    # Raise an error if no file found
    if len(ls_dir) == 0:
        print('No File Found')
        sys.exit()

    # ls_dir = [x for x in ls_dir if (x[-5]=='0') or (x[-5]=='5')]

    # Run the funtion using multiprocessing
    result = pool.map(transform_csv, ls_dir)

    # Print process time
    print('That took {} minutes'.format((datetime.now() - tStart)/60))

    # Exit
    sys.exit()



#---------------------------------------

# import tu_pb_to_csv
# import tu_csv_transform
# from datetime import datetime
# import glob
# import multiprocessing
# import shutil
# import os
#
# # tu_pb_to_csv.tu_pb_to_csv('/Users/txia0093/Desktop', 'test_pb', 'raw_csv', 'GTFS_TU2020-10-14_08_00')
# # tu_pb_to_csv.tu_pb_to_csv('/Volumes/research-data/PRJ-SCALUT/GTFS_tim/10_raw_pb_tu/gtfs_tu_2020_06_01/gtfs_tu_2020_06_01_00_00.pb.gz','')
#
# # tu_pb_to_csv.tu_pb_to_csv_folder('/Volumes/research-data/PRJ-SCALUT/GTFS_tim/10_raw_pb_tu/gtfs_tu_2020_06_01/','')
#
# dir_ori = '/Volumes/research-data/PRJ-SCALUT/GTFS_tim/10_raw_pb_tu/gtfs_tu_2020_06_01/'
#
# if dir_ori[-1:] != '/':
#     dir_ori += '/'
#
# def pd_to_csv(src):
#     tu_pb_to_csv.tu_pb_to_csv(src,'')
#
# def chunks(l, n):
#     for i in range(0, len(l), n):
#         yield l[i:i + n]
#
# numberOfThreads = 100
#
# if __name__ == '__main__':
#     tStart = datetime.now()
#     processes = []
#     ls_dir = glob.glob(os.path.join(dir_ori + '*.pb.gz'))
#     if len(ls_dir) == 0:
#         print('No File Found')
#
#     for i in range(0,len(ls_dir)):
#         p = multiprocessing.Process(target=pd_to_csv, args=(ls_dir[i],))
#         processes.append(p)
#
#     for i in chunks(processes,numberOfThreads):
#         for j in i:
#             j.start()
#         for j in i:
#             j.join()
#
#     print('That took {} minutes'.format((datetime.now() - tStart).total_seconds()/60))

# tu_csv_transform.tu_csv_transform('/Volumes/research-data/PRJ-SCALUT/GTFS_tim/11_csv_raw_tu/gtfs_tu_2020_06_01/gtfs_tu_2020_06_01_00_00.csv','')

# import tu_pb_to_csv
# import tu_csv_transform
# from datetime import datetime
# import glob
# import multiprocessing
# import os
# from itertools import repeat
#
# dir_ori = '/Volumes/research-data/PRJ-SCALUT/GTFS_tim/10_raw_pb_tu/gtfs_tu_2020_06_01/'
#
# if dir_ori[-1:] != '/':
#     dir_ori += '/'
#
# def pd_to_csv(src):
#     tu_pb_to_csv.tu_pb_to_csv(src,'')
#
# # if __name__ == '__main__':
# #     tStart = datetime.now()
# #     processes = []
# #     ls_dir = glob.glob(os.path.join(dir_ori + '*.pb.gz'))
# #     if len(ls_dir) == 0:
# #         print('No File Found')
# #
# #     with multiprocessing.Pool() as pool:
# #         pool.starmap(tu_pb_to_csv, zip(ls_dir, repeat('')))
# #
# #     print('That took {} minutes'.format((datetime.now() - tStart).total_seconds()/60))
#
# ls_dir = glob.glob(os.path.join(dir_ori + '*.pb.gz'))
# # print(zip(ls_dir, repeat('')))
# for i in zip(ls_dir, repeat('')):
#     print(i)
