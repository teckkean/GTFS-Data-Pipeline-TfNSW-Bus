'''
This function converts the SINGLE protobuf formated GTFS-Realtime Trip Updates file into fully cleaned csv file.
This code use the TfNSW specific proto file for processing.

Totally 2 inputs required:
dir_ori: The path to the .pb file.
dir_dest: The output path. If no input is given, the best estimate of the output path will be used.
'''

# import packages
import os
import gzip
import json
import pandas as pd
from pandas import json_normalize
from google.protobuf.json_format import MessageToJson
import flat_table
from datetime import datetime
import csv
from pytz import timezone
import pytz
import multiprocessing

# Try to input tfnsw_gtfs_realtime_pb2.py.
# Raise notification if tfnsw_gtfs_realtime_pb2.py is not found.
try:
    import tfnsw_gtfs_realtime_pb2
except:
    print('TfNSW specific proto file is required in this directory! Please make sure the file tfnsw_gtfs_realtime_pb2.py is in the same directory')

# Start main function
def tu_pd_to_clean_csv(dir_ori,dir_dest=''):

    # Check whether a folder is imported instead of a file.
    # Make sure the input dir_ori is only for a single file.
    if dir_ori[-6:] != '.pb.gz':
        print('A file path is required as the first input, not a folder path')
        return

    # Stop if no original file found
    if not os.path.exists(dir_ori):
        print('No original file found!')
        return

    # Get full_file_name
    full_file_name = dir_ori.split('/')[-1]

    # If destination path is not given, generated the best estimate of output path.
    if dir_dest == '':
        class_layer = dir_ori.split('/')[-4]
        dir_dest = dir_ori.replace(class_layer,'12_csv_transformed_tu')
        full_file_name = dir_dest.split('/')[-1]
        dir_dest_folder = dir_dest.replace(full_file_name,'')[:-1]
        file_name = full_file_name[:-6]
        dir_dest = dir_dest_folder + '/' + file_name + '.csv.gz'

    # If the output is found, stop the process and return.
    if os.path.exists(dir_dest):
        print(full_file_name,' exist')
        return

    # Check if destination folder exists. Create if not.
    if not os.path.exists(dir_dest_folder+'/'):
        print('Create Folder: ',dir_dest_folder)
        os.makedirs(dir_dest_folder, exist_ok=True)

    # Set up UTC
    utc = pytz.utc

    # Generated protobuf feed.
    feed = tfnsw_gtfs_realtime_pb2.FeedMessage()

    ## Record Start Time
    tStart = datetime.now()

    # Open .PB.GZ file
    with gzip.open(dir_ori) as f:
        feed.ParseFromString(f.read())

    # ENTITY DATA
    entitylist = [entity for entity in feed.entity]

    # Protobof to JSON
    jsonObj = MessageToJson(feed,preserving_proto_field_name=True)
    data = json.loads(jsonObj)

    # Normalise JSON to Dataframe
    df_js = json_normalize(data)
    ## Get Header Information
    HeaderTS = df_js._get_value(0,'header.timestamp')

    ## Convert 'headerTS' from str to int
    iHeaderTS = int(HeaderTS)
    ## Convert 'headerTS' from POSIX Time to UTC Time
    UTC_HeaderTS = utc.localize(datetime.utcfromtimestamp(iHeaderTS)).astimezone(timezone('Australia/Sydney')).strftime('%Y-%m-%d %H:%M:%S')

    ## JSON 'Entity' to Dataframe
    df= json_normalize(data, 'entity')
    df= flat_table.normalize(df)
    # df.head(2)

    ## Sort Columns
    df = df[['id',
                             'trip_update.trip.trip_id',
                             'trip_update.trip.start_time',
                             'trip_update.trip.start_date',
                             'trip_update.trip.schedule_relationship',
                             'trip_update.trip.route_id',
                             'trip_update.vehicle.id',
                             'trip_update.timestamp',
                             'trip_update.stop_time_update.stop_sequence',
                             'trip_update.stop_time_update.arrival.delay',
                             'trip_update.stop_time_update.arrival.time',
                             'trip_update.stop_time_update.departure.delay',
                             'trip_update.stop_time_update.departure.time',
                             'trip_update.stop_time_update.stop_id',
                             'trip_update.stop_time_update.schedule_relationship']]

    ## Add Time Columns
    df['request_time_dt'] = UTC_HeaderTS
    df['trip_update.timestamp_dt'] = pd.DatetimeIndex(
        pd.to_datetime(df['trip_update.timestamp'],unit='s'),tz='UTC').tz_convert(
        'Australia/Sydney').tz_localize(None)
    df['trip_update.stop_time_update.arrival.time_dt'] = pd.DatetimeIndex(
        pd.to_datetime(df['trip_update.stop_time_update.arrival.time'],unit='s'),tz='UTC').tz_convert(
        'Australia/Sydney').tz_localize(None)
    df['trip_update.stop_time_update.departure.time_dt'] = pd.DatetimeIndex(
        pd.to_datetime(df['trip_update.stop_time_update.departure.time'],unit='s'),tz='UTC').tz_convert(
        'Australia/Sydney').tz_localize(None)
    df['trip_update.trip.start_time_dt'] = pd.to_datetime(df['trip_update.trip.start_date'],
                                                                      format='%Y%m%d') + pd.to_timedelta(
        df['trip_update.trip.start_time'])

    # Drop unused columns
    df = df.drop(columns=['trip_update.trip.start_time','trip_update.trip.start_date','trip_update.timestamp',
                      'trip_update.stop_time_update.arrival.time','trip_update.stop_time_update.departure.time'])
    # Clean column names
    df.columns = [x.replace('trip_update.','').replace('stop_time_update.','stop_').replace('.','_') for x in list(df.columns)]
    df.columns = [x.replace('trip_trip_id','trip_id').replace('trip_route_id','route_id').replace('stop_stop_sequence','stop_sequence').replace('stop_stop_id','stop_id') for x in list(df.columns)]

    # Clean column data type
    df['stop_schedule_relationship'] = df['stop_schedule_relationship'].astype(str)
    df['trip_schedule_relationship'] = df['trip_schedule_relationship'].astype(str)

    # Filter by unuseable data
    df = df[(df['stop_schedule_relationship'] != 'NO_DATA')]
    df =  df[(df['trip_schedule_relationship'] != 'CANCELED')
        & (df['trip_schedule_relationship'] != 'UNSCHEDULED')
        & (df['trip_schedule_relationship'] != 'NO_DATA')
        & (df['trip_schedule_relationship'] != 0)]

    # Clean column data type
    df['route_id'] = df['route_id'].astype(str)
    df['trip_id'] = df['trip_id'].astype(str)
    df = df[df['stop_sequence'].notnull()]
    df['stop_sequence'] = df['stop_sequence'].astype(int)

    # Sort DataFrame
    df = df.sort_values(by=['trip_id','timestamp_dt'])

    # Drop duplicates
    df = df.drop_duplicates(subset=['route_id','trip_id',
                                                'trip_start_time_dt','stop_sequence'], keep='last')


    # Output to CSV
    df.to_csv(dir_dest, index=False, compression='gzip')

    # Get current time
    tEnd = datetime.now()

    # Print information
    print(full_file_name[:-6], 'is completed at:', tEnd.isoformat(' ', 'seconds') + '; Run Time:', tEnd-tStart)

# tu_pd_to_clean_csv('/Users/txia0093/Desktop/test_pb/2021_01/gtfs_tu_2021_01_09/GTFS_TU_2021_01_09_08_00.pb.gz')
