'''
This function merges a folder of csv files into one daily csv file.
Each folder suggest to contain one day GTFS data.
This code use the multiprocessing function for a faster processing.

Totally 2 required:

1.  The input path to the csv folder.
    The input path should be inputed after calling the file name 'tu_merge_csv.py',
    as the second System Specific Parameter (sys.argv[1])

2.  The output path.
    The output path should be inputed after the first input,
    as the third System Specific Parameter (sys.argv[2])
'''

# import packages
import sys
import glob
import os
import pandas as pd
from datetime import datetime

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

## Record Start Time
tStart = datetime.now()

# Get file paths within the csv folder
ls_dir = glob.glob(os.path.join(dir_ori + '*.csv.gz'))

# Raise an error if no file found
if len(ls_dir) == 0:
    print('No File Found')
    sys.exit()

# Sort ls_dir
ls_dir.sort()

# Read the first csv file
df = pd.read_csv(ls_dir[0],compression='gzip')
df['trip_id'] = df['trip_id'].astype(str)
df['stop_sequence'] = df['stop_sequence'].astype(int)
# Sort the dataframe
df = df.sort_values(by=['id','stop_sequence'])

# Loop from the second csv file within the folder
for i in range(1,len(ls_dir)):
    # Read the csv file
    this_df = pd.read_csv(ls_dir[i],compression='gzip')
    this_df['trip_id'] = this_df['trip_id'].astype(str)
    this_df['stop_sequence'] = this_df['stop_sequence'].astype(int)
    this_df = this_df.sort_values(by=['id','stop_sequence'])
    # Concat with existing data
    # Only keep the latest trip update.
    df = pd.concat([df,this_df],ignore_index=True).drop_duplicates(['trip_id', 'stop_sequence'],keep='last')

    # Print progress
    if i % int(len(ls_dir)/10) == 0:
        print((i // int(len(ls_dir)/10))*10,'% Complete!')

# Save csv file
df.to_csv(dir_dest,index=False,compression='gzip')

# Get current time
tEnd = datetime.now()

# Print information
print(full_file_name, 'is completed at:', tEnd.isoformat(' ', 'seconds') + '; Run Time:', tEnd-tStart)
