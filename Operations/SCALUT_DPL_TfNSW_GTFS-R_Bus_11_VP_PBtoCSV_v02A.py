#!/usr/bin/env python
'''
Smart City Applications in Land Use and Transport (SCALUT)
TfNSW GTFS-R Bus Vehicle Positions

1.1 Convert .PB.GZ to .CSV Files

$ python SCALUT_DPL_TfNSW_GTFS-R_Bus_11_VP_PBtoCSV_v02A.py "DataDir" "FileTP"
EXAMPLE: python SCALUT_DPL_TfNSW_GTFS-R_Bus_11_VP_PBtoCSV_v02A.py "/scratch/RDS-FEI-SCALUT-RW/TfNSW_GTFS_Buses" "2020m10d01" 
'''

### Housekeeping: Import Libraries/Packages
import sys
import os
import tfnsw_gtfs_realtime_pb2  # TfNSW specific proto file is required in this directory 
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
import glob

### Specify Project Directory and Folders and Define Variables

## Specify the folder that stores the .PB.GZ files to be processed
FileTP = sys.argv[2]

## Specify the GTFS-R file prefix
GTFS_VP_Prefix = 'GTFS_VP'

## Specifiy the main directory that stores input and output folders
DataDir = sys.argv[1]

## Specifiy the main folders that stores input and output data
FldRawPB = '10_Raw_PB'
FldRawCSVvp = '11_CSV_Raw_VP'

####################################################################################################

### Read and Process .PB.GZ Files (VP)

## Directory Path
DirRawPB = DataDir + '/' + FldRawPB + '/' + FileTP
DirRawCSVvp = DataDir + '/' + FldRawCSVvp + '/' + FileTP
if not os.path.exists(DirRawCSVvp):
    os.makedirs(DirRawCSVvp)

## Define File Name and Path for CSV Summary File
CSVsummaryVP = 'Summary_VP_' + FileTP
FilePathCSVsumVP = DirRawCSVvp + '/' + CSVsummaryVP + '.csv'
## Check if compare file exists. Remove if exist.
if os.path.exists(FilePathCSVsumVP):
    os.remove(FilePathCSVsumVP)

utc = pytz.utc
feed = tfnsw_gtfs_realtime_pb2.FeedMessage()

## Record Start Time
tStart = datetime.now()

iFile = 0

with open(FilePathCSVsumVP, mode='w', newline='') as csv_file:
    FieldNames = ['FileName', 'Entity', 'FileTS', 'Header.TS', 'Header.GTFSRversion', 'Header.Incrementality', 'Record']
    writer = csv.DictWriter(csv_file, fieldnames=FieldNames)
    writer.writeheader()

    ## Looping .PB.GZ files saved in DirRawPB
    all_Raw = glob.glob(os.path.join(DirRawPB, GTFS_VP_Prefix + '*.pb.gz'))
    for FilePathRaw in all_Raw:
        ## Get FullFileName from Path
        strFullFileName = os.path.split(FilePathRaw)[1]
        ##print(strFullFileName)

        ## FileName exclude Extension
        FNexExt = strFullFileName[0:-6]
        ## Timestamp from FileName
        fileTS = FNexExt[-16:]

        ## Open .PB.GZ file
        with gzip.open(FilePathRaw) as f:
            feed.ParseFromString(f.read())

        # ## OPTIONAL: DISPLAY HEADER DATA
        # HeaderData = feed.header
        # HeaderData

        ## ENTITY DATA
        entitylist = [entity for entity in feed.entity]
        # print(len(entitylist))

        # ## OPTIONAL: DISPLAY ENTITY DATA
        # entitylist[0]

        ## Protobof to JSON
        jsonObj = MessageToJson(feed, preserving_proto_field_name=True)
        data = json.loads(jsonObj)

        ## Normalise JSON to Dataframe
        df_js = json_normalize(data)
        ## Get Header.Timestamp Value
        HeaderGTFSver = df_js._get_value(0, 'header.gtfs_realtime_version')
        HeaderIncr = df_js._get_value(0, 'header.incrementality')
        HeaderTS = df_js._get_value(0, 'header.timestamp')

        ## Convert 'headerTS' from str to int 
        iHeaderTS = int(HeaderTS)
        ## Convert 'headerTS' from POSIX Time to UTC Time
        UTC_HeaderTS = utc.localize(datetime.utcfromtimestamp(iHeaderTS)).astimezone(
            timezone('Australia/Sydney')).strftime('%Y-%m-%d %H:%M:%S')

        ## JSON 'Entity' to Dataframe
        df_VP = json_normalize(data, 'entity')
        df_VPft = flat_table.normalize(df_VP)
        # df_VPft.head(2)

        ## Drop Columns
        df_GTFS_VP = df_VPft.drop(['index'], 1)

        ## Sort Columns
        df_GTFS_VP = df_GTFS_VP[['id',
                                 'vehicle.trip.trip_id',
                                 'vehicle.trip.start_time',
                                 'vehicle.trip.start_date',
                                 'vehicle.trip.schedule_relationship',
                                 'vehicle.trip.route_id',
                                 'vehicle.position.latitude',
                                 'vehicle.position.longitude',
                                 'vehicle.position.bearing',
                                 'vehicle.position.speed',
                                 'vehicle.timestamp',
                                 'vehicle.congestion_level',
                                 'vehicle.vehicle.id',
##                                 'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].air_conditioned',
##                                 'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].wheelchair_accessible',
##                                 'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].vehicle_model',
##                                 'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].performing_prior_trip',
##                                 'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].special_vehicle_attributes',
                                 'vehicle.occupancy_status']]

        ## Add New Columns
        df_GTFS_VP['VPheaderTS'] = UTC_HeaderTS
        df_GTFS_VP['vehicle.timestampUTC'] = pd.DatetimeIndex(pd.to_datetime(df_GTFS_VP['vehicle.timestamp'], unit='s'),
                                                              tz='UTC').tz_convert('Australia/Sydney').tz_localize(None)
        df_GTFS_VP['vehicle.trip.start_DateTimeUTC'] = pd.to_datetime(df_GTFS_VP['vehicle.trip.start_date'],
                                                                      format='%Y%m%d') + pd.to_timedelta(
            df_GTFS_VP['vehicle.trip.start_time'])
        # df_GTFS_VP.head(2)

        ## Output to CSV
        df_GTFS_VP.to_csv(DirRawCSVvp + '/' + FNexExt + '.csv', index=False)

        ## Write data to CSV Summary
        writer.writerow({'FileName': strFullFileName,
                         'Entity': len(entitylist),
                         'FileTS': fileTS,
                         'Header.TS': UTC_HeaderTS,
                         'Header.GTFSRversion': HeaderGTFSver,
                         'Header.Incrementality': HeaderIncr,
                         'Record': df_GTFS_VP.shape[0]})

        ## Count File
        iFile = iFile + 1

## Record End Time
tEnd = datetime.now()
print(iFile, 'Files Completed:', tEnd.isoformat(' ', 'seconds') + '; Time Spent:', tEnd - tStart)
print('CSV Summary file saved in:', FilePathCSVsumVP)
