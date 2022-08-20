'''
This function renames the columns and cleans up the converted csv file by removing unreasonable results.

Totally 2 inputs required:
dir_ori: The path to the .csv file.
dir_dest: The output path. If no input is given, the best estimate of the output path will be used.
'''

# import packages
import sys
import os
from datetime import datetime
import pandas as pd
import glob
import time
import numpy as np

# Start main function
def tu_transform_csv(dir_ori,dir_dest=''):

    # Check whether a folder is imported instead of a file.
    # Make sure the input dir_ori is only for a single csv file.
    if dir_ori[-7:] != '.csv.gz':
        print('A file path is required as the first input, not a folder path')
        return

    # NOT IN USE:
    # dir_static: The path to GTFS static data. If no input is given, the best estimate of the output path will be used.
    # If GTFS static path is not given, generated the best estimate of output path.
    # if dir_static == '':
    #     class_layer = dir_ori.split('/')[-3]
    #     date_layer = dir_ori.split('/')[-2]
    #     dir_static = dir_ori.replace(class_layer,'gtfs_static')
    #     full_file_name = dir_static.split('/')[-1]
    #     dir_static_folder = dir_static.replace(date_layer+'/'+full_file_name,'')[:-1]
    #     date_folder = full_file_name[:-13][8:].replace('_','')
    #     dir_static = dir_static_folder + '/' + date_folder

    # Get full_file_name
    full_file_name = dir_ori.split('/')[-1]

    # If destination path is not given, generated the best estimate of output path.
    if dir_dest == '':
        class_layer = dir_ori.split('/')[-4]
        dir_dest = dir_ori.replace(class_layer,'12_csv_transformed_tu')
        full_file_name = dir_dest.split('/')[-1]
        dir_dest_folder = dir_dest.replace(full_file_name,'')[:-1]
        file_name = full_file_name[:-7]
        dir_dest = dir_dest_folder + '/' + file_name + '.csv.gz'

    # If the output is found, stop the process and return.
    if os.path.exists(dir_dest):
        print(full_file_name,' exist')
        return

    # print('Original Directory:',dir_ori)
    # print('GTFS Static Directory:',dir_static)
    # print('Destination Directory:',dir_dest)
    # print('Destination Folder:',dir_dest_folder)

    # Stop if no original file found
    if not os.path.exists(dir_ori):
        print('No original file found!')
        return

    # Stop if no GTFS static file found
    # if not os.path.exists(dir_static+'/'):
    #     print('No GTFS static file found!')
    #     return

    # Check if destination folder exists. Create if not.
    if not os.path.exists(dir_dest_folder):
        os.makedirs(dir_dest_folder, exist_ok=True)
        print('Destination Folder Created: ',dir_dest_folder)

    # Stop if no Stop List file found
    # if not os.path.exists(dir_static + '/ls_stops_used.npy'):
    #     print('No Stop List file found!')
    #     return

    # NOT IN USE: Read Stop List
    # ls_stops = np.load(dir_static + '/ls_stops_used.npy')

    # df_st_times = pd.read_csv(dir_static+'/stop_times.txt',dtype={'trip_id':'str','arrival_time':'str','departure_time':'str','stop_id':'str',
    #                             'stop_sequence':'Int64','stop_headsign':'str','pickup_type':'int','drop_off_type':'int',
    #                             'shape_dist_traveled':'float','timepoint':'int','stop_note':'str'})

    # Record Start Time
    tStart = datetime.now()

    # Read the raw csv file
    df = pd.read_csv(dir_ori, sep=',', dtype={'id':'str',
                                                   'trip_update.trip.trip_id':'str',
                                                   'trip_update.trip.start_time':'str',
                                                   'trip_update.trip.start_date':'str',
                                                   'trip_update.trip.schedule_relationship':'str',
                                                   'trip_update.trip.route_id':'str',
                                                   'trip_update.vehicle.id':'str',
                                                   'trip_update.timestamp':'Int64',
                                                   'trip_update.stop_time_update.stop_sequence':'Int64',
                                                   'trip_update.stop_time_update.arrival.delay':'Int64',
                                                   'trip_update.stop_time_update.arrival.time':'Int64',
                                                   'trip_update.stop_time_update.departure.delay':'Int64',
                                                   'trip_update.stop_time_update.departure.time':'Int64',
                                                   'trip_update.stop_time_update.stop_id':'str',
                                                   'trip_update.stop_time_update.schedule_relationship':'str',
                                                   'request_time_dt':'str',
                                                   'trip_update.timestamp_dt':'str',
                                                   'trip_update.stop_time_update.arrival.time_dt':'str',
                                                   'trip_update.stop_time_update.departure.time_dt':'str',
                                                   'trip_update.trip.start_time_dt':'str'},
                                parse_dates=['trip_update.trip.start_time_dt'], compression='gzip')


    # Drop unused columns
    df = df.drop(columns=['trip_update.trip.start_time','trip_update.trip.start_date','trip_update.timestamp',
                      'trip_update.stop_time_update.arrival.time','trip_update.stop_time_update.departure.time'])
    # Clean column names
    df.columns = [x.replace('trip_update.','').replace('stop_time_update.','stop_').replace('.','_') for x in list(df.columns)]
    df.columns = [x.replace('trip_trip_id','trip_id').replace('trip_route_id','route_id').replace('stop_stop_sequence','stop_sequence').replace('stop_stop_id','stop_id') for x in list(df.columns)]

    # df['tu_stop_id'] = df['tu_stop_id'].astype(str)

    # Filter by stop list
    # df = df[df['tu_stop_id'].isin(ls_stops)]

    # Filter by unuseable data
    df = df[(df['stop_schedule_relationship'] != 'NO_DATA')]
    df =  df[(df['trip_schedule_relationship'] != 'CANCELED')
        & (df['trip_schedule_relationship'] != 'UNSCHEDULED')
        & (df['trip_schedule_relationship'] != 'NO_DATA')
        & (df['trip_schedule_relationship'] != 0)]
    df = df[df['stop_sequence'].notnull()]

    # Sort DataFrame
    df = df.sort_values(by=['trip_id','timestamp_dt'])

    # For maintenance only
    # print(len(df))

    # Drop duplicates
    df = df.drop_duplicates(subset=['route_id','trip_id',
                                                'trip_start_time_dt','stop_sequence'], keep='last')

    # For maintenance only
    # print(len(df))

    # Save to csv
    df.to_csv(dir_dest,index=False, compression='gzip')

    # Print information
    tEnd = datetime.now()
    print(full_file_name, 'is completed at:', tEnd.isoformat(' ', 'seconds') + '; Run Time:', tEnd-tStart)




    # ----- Unused Cleaning Up Sections -----
    # df =  df[
    #     (df['trip_update.trip.schedule_relationship'] != 'CANCELED')
    #     & (df['trip_update.trip.schedule_relationship'] != 'UNSCHEDULED')
    #     & (df['trip_update.stop_time_update.schedule_relationship'] != 'NO_DATA')
    #     & (df['trip_update.stop_time_update.arrival.time'] != 0)]
    #
    # df['Rt.Scheduled_Arrival.Time'] = df['trip_update.stop_time_update.arrival.time'] - df['trip_update.stop_time_update.arrival.delay']
    # df['Rt.Scheduled_Arrival.Time_dt'] = pd.DatetimeIndex(
    #     pd.to_datetime(df['Rt.Scheduled_Arrival.Time'],unit='s'),tz='UTC').tz_convert(
    #     'Australia/Sydney').tz_localize(None)
    # df['Rt.Scheduled_Arrival.TimeUTC'] = df['Rt.Scheduled_Arrival.TimeUTC'].dt.time
    # df = df.astype({'Rt.Scheduled_Arrival.TimeUTC':'str'})
    # df['trip_update.trip.trip_id2'] = df['trip_update.trip.trip_id'].str.rsplit('_', 1).str.get(0)
    # df['Rt.Scheduled_Arrival.TimeUTC2'] = df['Rt.Scheduled_Arrival.TimeUTC'].str.split(':').apply(lambda x:'%s:%s:%s' % (x[0] if int(x[0])>=4 else int(x[0])+24,x[1],x[2]))
    #
    # df = pd.merge(df,
    #                      df_st_times[['trip_id','arrival_time','stop_id','stop_sequence']],
    #                      how='left',
    #                      suffixes=('','_ST2'),
    #                      left_on=['trip_update.trip.trip_id2','trip_update.stop_time_update.stop_id','Rt.Scheduled_Arrival.TimeUTC2'],
    #                      right_on=['trip_id','stop_id','arrival_time'])
    # df['stop_sequence'].fillna(df['trip_update.stop_time_update.stop_sequence'], inplace=True)
    #
    # ## Fill Empty Stop Sequence Using Existing Data
    # df = df.astype({'stop_sequence':'float'})
    # df['stop_sequence'] = df.groupby(['trip_update.trip.route_id',
    #                                                 'trip_update.trip.trip_id',
    #                                                 'trip_update.trip.start_DateTimeUTC',
    #                                                 'trip_update.stop_time_update.stop_id'
    #                                                ])['stop_sequence'].apply(lambda x:x.fillna(x.mean()))
    # df['stop_sequence'] = df['stop_sequence'].round(0).astype('Int64')
    #
    # ## Drop Columns
    # df = df.drop(columns=['trip_id','arrival_time','stop_id'])
    #
    # df = pd.merge(df,
    #                      df_st_times[['trip_id','stop_id','stop_sequence','shape_dist_traveled']],
    #                      how='left',
    #                      suffixes=('','_ST3'),
    #                      left_on=['trip_update.trip.trip_id2','trip_update.stop_time_update.stop_id','stop_sequence'],
    #                      right_on=['trip_id','stop_id','stop_sequence'])
    # ## Drop Columns
    # df = df.drop(columns=['trip_id','stop_id'])
    #
    # df.sort_values(by=['trip_update.trip.route_id',
    #                           'trip_update.trip.trip_id',
    #                           'trip_update.trip.start_DateTimeUTC',
    #                           'stop_sequence',
    #                           'trip_update.timestamp'
    #                          ], inplace=True)
    #
    # df['Bad_Flag0'] = df.groupby(['trip_update.trip.route_id',
    #                                             'trip_update.trip.trip_id',
    #                                             'trip_update.trip.start_DateTimeUTC',
    #                                             'stop_sequence'])['trip_update.timestamp'].diff().ge(240).fillna(0)*1
    #
    # df['Bad_Flag1'] = df.groupby(['trip_update.trip.route_id',
    #                                             'trip_update.trip.trip_id',
    #                                             'trip_update.trip.start_DateTimeUTC',
    #                                             'stop_sequence'])['Bad_Flag0'].cumsum()
    # df = df[df['Bad_Flag1'] == 0]
    # ## Drop Columns
    # df = df.drop(columns=['Bad_Flag0','Bad_Flag1'])

# For maintenance only:
# tu_csv_transform('/Users/txia0093/Desktop/11_csv_raw_tu/2021_01/gtfs_tu_2021_01_09/gtfs_tu_2021_01_09_00_05.csv.gz')
