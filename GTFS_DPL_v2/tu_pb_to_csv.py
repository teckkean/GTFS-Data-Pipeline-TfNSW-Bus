'''
This function converts the SINGLE protobuf formated GTFS-Realtime Trip Updates file into csv file.
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
def tu_pb_to_csv(dir_ori,dir_dest=''):

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
        dir_dest = dir_ori.replace(class_layer,'11_csv_raw_tu')
        full_file_name = dir_dest.split('/')[-1]
        dir_dest_folder = dir_dest.replace(full_file_name,'')[:-1]
        file_name = full_file_name[:-6]
        dir_dest = dir_dest_folder + '/' + file_name + '.csv.gz'

    # If the output is found, stop the process and return.
    if os.path.exists(dir_dest):
        print(full_file_name,' exist')
        return

    # For maintenance only
    # print(dir_dest_folder)
    # print(file_name)

    # Check if destination folder exists. Create if not.
    if not os.path.exists(dir_dest_folder+'/'):
        print('Create Folder: ',dir_dest_folder)
        os.makedirs(dir_dest_folder, exist_ok=True)

    # Create summary folder path.
    date_layer = dir_dest_folder.split('/')[-1]
    csv_summary_folder = dir_dest_folder.replace(date_layer,'')[:-1]+'/summary'

    # For maintenance only
    # print(date_layer)
    # print(csv_summary_folder)

    # Create summary folder path if summary folder is not found.
    if not os.path.exists(csv_summary_folder+'/'):
        os.makedirs(csv_summary_folder, exist_ok=True)

    ## Define File Name and Path for Comparison File
    csv_summary_file_path = csv_summary_folder + '/' + file_name[:-6] + '.csv'

    # print(csv_summary_file_path)

    # ## Check if compare file exists. Remove if exist.
    # if os.path.exists(csv_summary_file_path):
    #     os.remove(csv_summary_file_path)

    # Set up UTC
    utc = pytz.utc

    # Generated protobuf feed.
    feed = tfnsw_gtfs_realtime_pb2.FeedMessage()

    ## Record Start Time
    tStart = datetime.now()

    ## Timestamp from FileName
    fileTS = file_name[-16:]
    # print('fileTS:',fileTS)

    ## Open .PB.GZ file
    with gzip.open(dir_ori) as f:
        feed.ParseFromString(f.read())

    # For maintenance only: DISPLAY HEADER DATA
    # HeaderData = feed.header
    # HeaderData

    ## ENTITY DATA
    entitylist = [entity for entity in feed.entity ]

    # For maintenance only: DISPLAY ENTITY DATA
    # print(entitylist[0])

    ## Protobof to JSON
    jsonObj = MessageToJson(feed,preserving_proto_field_name=True)
    data = json.loads(jsonObj)

    ## Normalise JSON to Dataframe
    df_js = json_normalize(data)
    ## Get Header Information
    HeaderGTFSver = df_js._get_value(0,'header.gtfs_realtime_version')
    HeaderIncr = df_js._get_value(0,'header.incrementality')
    HeaderTS = df_js._get_value(0,'header.timestamp')

    ## Convert 'headerTS' from str to int
    iHeaderTS = int(HeaderTS)
    ## Convert 'headerTS' from POSIX Time to UTC Time
    UTC_HeaderTS = utc.localize(datetime.utcfromtimestamp(iHeaderTS)).astimezone(timezone('Australia/Sydney')).strftime('%Y-%m-%d %H:%M:%S')

    ## JSON 'Entity' to Dataframe
    df_TU = json_normalize (data, 'entity')
    df_TUft = flat_table.normalize(df_TU)
    # df_TUft.head(2)

    # Sort Data by 'index','arrival.time','stop_sequence'
    # df_TUft.sort_values(by=['index',
    #                         'trip_update.stop_time_update.arrival.time',
    #                         'trip_update.stop_time_update.stop_sequence'],
    #                    inplace=True)

    ## Sort Columns
    df_GTFS_TU = df_TUft[['id',
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
    df_GTFS_TU['request_time_dt'] = UTC_HeaderTS
    df_GTFS_TU['trip_update.timestamp_dt'] = pd.DatetimeIndex(
        pd.to_datetime(df_GTFS_TU['trip_update.timestamp'],unit='s'),tz='UTC').tz_convert(
        'Australia/Sydney').tz_localize(None)
    df_GTFS_TU['trip_update.stop_time_update.arrival.time_dt'] = pd.DatetimeIndex(
        pd.to_datetime(df_GTFS_TU['trip_update.stop_time_update.arrival.time'],unit='s'),tz='UTC').tz_convert(
        'Australia/Sydney').tz_localize(None)
    df_GTFS_TU['trip_update.stop_time_update.departure.time_dt'] = pd.DatetimeIndex(
        pd.to_datetime(df_GTFS_TU['trip_update.stop_time_update.departure.time'],unit='s'),tz='UTC').tz_convert(
        'Australia/Sydney').tz_localize(None)
    df_GTFS_TU['trip_update.trip.start_time_dt'] = pd.to_datetime(df_GTFS_TU['trip_update.trip.start_date'],
                                                                      format='%Y%m%d') + pd.to_timedelta(
        df_GTFS_TU['trip_update.trip.start_time'])
    # df_GTFS_TU.head(2)

    # For maintenance only:
    # print('dir_dest_folder:',dir_dest_folder)
    # print('dir_dest_path:',dir_dest)

    # Output to CSV
    df_GTFS_TU.to_csv(dir_dest, index=False, compression='gzip')


    # If the summary file doesn't exist, create the summary file.
    if os.path.exists(csv_summary_file_path):
        with open(csv_summary_file_path, 'a', newline='') as csv_file:
            FieldNames = ['FileName','Entity','FileTS','TS','GTFSRversion','Incrementality','Record','Run Time','Finished Time']
            writer = csv.DictWriter(csv_file, fieldnames=FieldNames)
            ## Write data to CSV Summary
            writer.writerow({'FileName':full_file_name,
                             'Entity':len(entitylist),
                             'FileTS':fileTS,
                             'TS':UTC_HeaderTS,
                             'GTFSRversion':HeaderGTFSver,
                             'Incrementality':HeaderIncr,
                             'Record':df_GTFS_TU.shape[0],
                             'Run Time': (tEnd-tStart).total_seconds(),
                             'Finished Time':datetime.now()})
    # If the summary file exists, append to the summary file.
    else:
        with open(csv_summary_file_path, 'w', newline='') as csv_file:
            FieldNames = ['FileName','Entity','FileTS','TS','GTFSRversion','Incrementality','Record','Run Time','Finished Time']
            writer = csv.DictWriter(csv_file, fieldnames=FieldNames)
            writer.writeheader()
            ## Write data to CSV Summary
            writer.writerow({'FileName':full_file_name,
                             'Entity':len(entitylist),
                             'FileTS':fileTS,
                             'TS':UTC_HeaderTS,
                             'GTFSRversion':HeaderGTFSver,
                             'Incrementality':HeaderIncr,
                             'Record':df_GTFS_TU.shape[0],
                             'Run Time': (tEnd-tStart).total_seconds(),
                             'Finished Time':datetime.now()})

    # Get current time
    tEnd = datetime.now()

    # Print information
    print(full_file_name, 'is completed at:', tEnd.isoformat(' ', 'seconds') + '; Run Time:', tEnd-tStart)
    print('Summary saved in:', csv_summary_file_path)

# For maintenance only:
# tu_pb_to_csv('/Users/txia0093/Desktop/test_pb/month_layer/date_layer/GTFS_TU2020-10-14_08_00.pb.gz')
