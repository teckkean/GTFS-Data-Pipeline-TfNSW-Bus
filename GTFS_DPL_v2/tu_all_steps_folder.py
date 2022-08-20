'''
This function converts a folder of protobuf files into fully cleaned csv files.
Each folder suggest to contain one day GTFS data.
This code use the multiprocessing function for a faster processing.

Totally 2 required:

1.  The input path to the protobuf folder.
    The input path should be inputed after calling the file name 'tu_transform_csv_folder.py',
    as the second System Specific Parameter (sys.argv[1])

2.  The output path.
    The output path should be inputed after the first input,
    as the third System Specific Parameter (sys.argv[2])
'''

# import packages
import tu_all_steps
import sys
import multiprocessing
from datetime import datetime
import glob
import os
import time
import pandas as pd

# Get csv folder directory
dir_ori = str(sys.argv[1])

# Check whether a file is imported instead of a folder.
# Make sure the input dir_ori is only for a csv folder.
if dir_ori[-6:] == '.pb.gz' or dir_ori[-3:] == '.pb':
    print('A folder path is required as the first input, not a file path')
    sys.exit()

# Add '/' to the folder of the path has no '/' at the end
if dir_ori[-1:] != '/':
    dir_ori += '/'

# Generate file name
full_file_name = dir_ori.split('/')[-2]+'_all.csv.gz'

# Get destination folder
class_layer = dir_ori.split('/')[-4]
date_layer = dir_ori.split('/')[-2] +'/'
dir_dest_folder = dir_ori.replace(class_layer,'daily_tu').replace(dir_ori.split('/')[-1],'').replace(date_layer,'')

# if destination path inputted, get output path.
if len(sys.argv) >2:
    dir_dest = str(sys.argv[2])
# if no destination path inputted, generated the best estimate of output path.
else:
    dir_dest = dir_dest_folder + full_file_name

# Check if destination folder exists. Create if not.
if not os.path.exists(dir_dest_folder):
    os.makedirs(dir_dest_folder, exist_ok=True)
    print('Destination Folder Created: ',dir_dest_folder)

# Get csv output Folder
dir_csv_folder = dir_ori.replace(class_layer,'12_csv_transformed_tu')


# Define the function for multiprocessing
def pd_to_csv(src):
    tu_all_steps.tu_pd_to_clean_csv(src,'')

# Start the parallel processing
if __name__ == '__main__':

    # Get start time for generating csv
    tStart = datetime.now()
    # Check number of CPUs
    np = multiprocessing.cpu_count()
    # Generate process list
    processes = []
    # Set up pool by the number of CPUs
    pool = multiprocessing.Pool(processes = np)
    # Get file paths within the csv folder
    ls_dir = glob.glob(os.path.join(dir_ori + '*.pb.gz'))

    # Raise an error if no file found
    if len(ls_dir) == 0:
        print('No File Found')
        sys.exit()

    # Run the funtion using multiprocessing
    result = pool.map(pd_to_csv, ls_dir)

    # Include time gap
    time.sleep(10)

    # Shut down multiprocessing
    try:
        pool.close()
        pool.join()
    except:
        print('Unable to shut doen multiprocessing')

    # Print process time
    print('Generating csv files took {} minutes'.format((datetime.now() - tStart)/60))

    # Get start time for merge csv
    tStart = datetime.now()

    # Get file paths within the csv folder
    ls_dir = glob.glob(os.path.join(dir_csv_folder + '*.csv.gz'))

    # Raise an error if no file found
    if len(ls_dir) == 0:
        print('No File Found')
        sys.exit()

    # Sort ls_dir
    ls_dir.sort()

    # print(ls_dir)

    # Read the first csv file
    df = pd.read_csv(ls_dir[0],compression='gzip',dtype={'id':'str',
                                                   'trip_id':'str',
                                                   'trip_schedule_relationship':'str',
                                                   'route_id':'str',
                                                   'vehicle_id':'str',
                                                   'stop_sequence':'Int64',
                                                   'stop_arrival_delay':'Int64',
                                                   'stop_departure_delay':'Int64',
                                                   'stop_id':'str',
                                                   'stop_schedule_relationship':'str',
                                                   'request_time_dt':'str',
                                                   'timestamp_dt':'str',
                                                   'stop_arrival_time_dt':'str',
                                                   'stop_departure_time_dt':'str',
                                                   'trip_start_time_dt':'str'})
    # df['trip_id'] = df['trip_id'].astype(str)
    # df['stop_sequence'] = df['stop_sequence'].astype(int)
    # Sort the dataframe
    df = df.sort_values(by=['id','stop_sequence'])

    # Loop from the second csv file within the folder
    for i in range(1,len(ls_dir)):
        # Read the csv file
        this_df = pd.read_csv(ls_dir[i],compression='gzip',dtype={'id':'str',
                                                       'trip_id':'str',
                                                       'trip_schedule_relationship':'str',
                                                       'route_id':'str',
                                                       'vehicle_id':'str',
                                                       'stop_sequence':'Int64',
                                                       'stop_arrival_delay':'Int64',
                                                       'stop_departure_delay':'Int64',
                                                       'stop_id':'str',
                                                       'stop_schedule_relationship':'str',
                                                       'request_time_dt':'str',
                                                       'timestamp_dt':'str',
                                                       'stop_arrival_time_dt':'str',
                                                       'stop_departure_time_dt':'str',
                                                       'trip_start_time_dt':'str'})
        # this_df['trip_id'] = this_df['trip_id'].astype(str)
        # this_df['stop_sequence'] = this_df['stop_sequence'].astype(int)
        this_df = this_df.sort_values(by=['id','stop_sequence'])
        # Concat with existing data
        # Only keep the latest trip update.
        df = pd.concat([df,this_df],ignore_index=True).drop_duplicates(['trip_id', 'stop_sequence'],keep='last')

        # Print progress
        if i % int(len(ls_dir)/10) == 0:
            print((i // int(len(ls_dir)/10))*10,'% Complete!')

    df = df.sort_values(by=['id','stop_sequence'])
    # Save csv file
    df.to_csv(dir_dest,index=False,compression='gzip')

    # Get current time
    tEnd = datetime.now()

    # Print information
    print(full_file_name, 'is completed at:', tEnd.isoformat(' ', 'seconds') + '; Run Time:', tEnd-tStart)

    # Exit
    sys.exit()
