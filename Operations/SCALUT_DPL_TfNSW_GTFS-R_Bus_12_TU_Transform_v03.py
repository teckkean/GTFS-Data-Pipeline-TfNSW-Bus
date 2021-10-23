#!/usr/bin/env python
'''
Smart City Applications in Land Use and Transport (SCALUT)
TfNSW GTFS-R Bus Trip Update 

1.2 Transform .CSV Files

$ python SCALUT_DPL_TfNSW_GTFS-R_Bus_12_TU_Transform_v03.py "DataDir" "FileTP" "FileIdStatic"
EXAMPLE: python SCALUT_DPL_TfNSW_GTFS-R_Bus_12_TU_Transform_v03.py "/scratch/RDS-FEI-SCALUT-RW/TfNSW_GTFS_Buses" "2020m10d01" "20201001191000"
'''

### Housekeeping: Import Libraries/Packages
import sys
import os
from datetime import datetime
import pandas as pd
import glob
import time
from zipfile import ZipFile

### Specify Project Directory and Folders and Define Variables

## Specifiy the main directory that stores input and output folders
DataDir = sys.argv[1]

## Specify the folder that stores the .PB.GZ files to be processed
FileTP = sys.argv[2]

# ## Specify the GTFS-R file prefix
GTFS_TU_Prefix = 'GTFS_TU'

## Specifiy the main folders that stores GTFS Static data
FldRawStatic = '10_Raw_Static'
FileIdStatic = sys.argv[3]

## Specifiy the main folders that stores input and output data
FldRawCSVtu = '11_CSV_Raw_TU'
FldTransTU = '12_CSV_Transformed_TU'
FldClnTU = '13_CSV_Cleaned_Unique_TU'

## Directory Path
DirRawCSVtu = DataDir + '/' + FldRawCSVtu + '/' + FileTP
if not os.path.exists(DirRawCSVtu):
    os.makedirs(DirRawCSVtu)

DirTransTU = DataDir + '/' + FldTransTU + '/' + FileTP
if not os.path.exists(DirTransTU):
    os.makedirs(DirTransTU)

DirClnTU = DataDir + '/' + FldClnTU + '/' + FileTP
if not os.path.exists(DirClnTU):
    os.makedirs(DirClnTU)

#################################################
#### Define Functions
#################################################
## Read Raw TU CSV
def Read_CSV_Raw_TU(f):
    df_CSV_Raw_TU = pd.read_csv(f, sep=',', dtype={'id':'str',
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
                                                   'TUheaderTS':'str',
                                                   'trip_update.timestampUTC':'str',
                                                   'trip_update.stop_time_update.arrival.timeUTC':'str',
                                                   'trip_update.stop_time_update.departure.timeUTC':'str',
                                                   'trip_update.trip.start_DateTimeUTC':'str'},
                                parse_dates=['trip_update.trip.start_DateTimeUTC'])
    return(df_CSV_Raw_TU)

#################################################
## Remove Redundant, Obsolete, Trivial Records
def Df_Remove_ROT(df):
    df_NoROT1 = df[
        (df['trip_update.trip.schedule_relationship'] != 'CANCELED') 
        & (df['trip_update.trip.schedule_relationship'] != 'UNSCHEDULED') 
        & (df['trip_update.stop_time_update.schedule_relationship'] != 'NO_DATA') 
        & (df['trip_update.stop_time_update.arrival.time'] != 0)
    ]
    return(df_NoROT1)

#################################################
## Calculate Scheduled ArrivalTime
def Df_SchArrTime(df_NoROT1):
    df_NoROT1['Rt.Scheduled_Arrival.Time'] = df_NoROT1['trip_update.stop_time_update.arrival.time'] - df_NoROT1['trip_update.stop_time_update.arrival.delay']
    df_NoROT1['Rt.Scheduled_Arrival.TimeUTC'] = pd.DatetimeIndex(
        pd.to_datetime(df_NoROT1['Rt.Scheduled_Arrival.Time'],unit='s'),tz='UTC').tz_convert(
        'Australia/Sydney').tz_localize(None)
    df_NoROT1['Rt.Scheduled_Arrival.TimeUTC'] = df_NoROT1['Rt.Scheduled_Arrival.TimeUTC'].dt.time
    df_NoROT1 = df_NoROT1.astype({'Rt.Scheduled_Arrival.TimeUTC':'str'})
    df_NoROT1['trip_update.trip.trip_id2'] = df_NoROT1['trip_update.trip.trip_id'].str.rsplit('_', 1).str.get(0)
    df_NoROT1['Rt.Scheduled_Arrival.TimeUTC2'] = df_NoROT1['Rt.Scheduled_Arrival.TimeUTC'].str.split(':').apply(lambda x:'%s:%s:%s' % (x[0] if int(x[0])>=4 else int(x[0])+24,x[1],x[2]))
    return(df_NoROT1)

#################################################
## Get Stop Sequence from GTFS Static
def Df_GetStaticStopSeq(df_NoROT1):
    df_NoROT2 = pd.merge(df_NoROT1,
                         df_StTimes[['trip_id','arrival_time','stop_id','stop_sequence']],
                         how='left',
                         suffixes=('','_ST2'),
                         left_on=['trip_update.trip.trip_id2','trip_update.stop_time_update.stop_id','Rt.Scheduled_Arrival.TimeUTC2'],
                         right_on=['trip_id','stop_id','arrival_time'])
    df_NoROT2['stop_sequence'].fillna(df_NoROT2['trip_update.stop_time_update.stop_sequence'], inplace=True)

    df_NoROT2 = df_NoROT2.astype({'stop_sequence':'float'})
    ## Fill Empty Stop Sequence Using Existing Data
    df_NoROT2['stop_sequence'] = df_NoROT2.groupby(['trip_update.trip.route_id', 
                                                    'trip_update.trip.trip_id', 
                                                    'trip_update.trip.start_DateTimeUTC',
                                                    'trip_update.stop_time_update.stop_id'
                                                   ])['stop_sequence'].apply(lambda x:x.fillna(x.mean()))
#    df_NoROT2['stop_sequence'].fillna(0)
    df_NoROT2['stop_sequence'] = df_NoROT2['stop_sequence'].round(0).astype('Int64')
#    df_NoROT2 = df_NoROT2.astype({'stop_sequence':'Int64'})

    ## Drop Columns
    df_NoROT2 = df_NoROT2.drop(columns=['trip_id','arrival_time','stop_id'])
    return(df_NoROT2)

#################################################
## Get shape_dist_traveled from GTFS Static
def Df_GetStaticDist(df_NoROT2):
    df_NoROT3 = pd.merge(df_NoROT2,
                         df_StTimes[['trip_id','stop_id','stop_sequence','shape_dist_traveled']],
                         how='left',
                         suffixes=('','_ST3'),
                         left_on=['trip_update.trip.trip_id2','trip_update.stop_time_update.stop_id','stop_sequence'],
                         right_on=['trip_id','stop_id','stop_sequence'])
    ## Drop Columns
    df_NoROT3 = df_NoROT3.drop(columns=['trip_id','stop_id'])
    return(df_NoROT3)

#################################################
## Flag Bad Observations
def Df_FlagBad(df_NoROT4):
    df_NoROT4.sort_values(by=['trip_update.trip.route_id',
                              'trip_update.trip.trip_id',
                              'trip_update.trip.start_DateTimeUTC',
                              'stop_sequence',
                              'trip_update.timestamp'
                             ], inplace=True)

    df_NoROT4['Bad_Flag0'] = df_NoROT4.groupby(['trip_update.trip.route_id', 
                                                'trip_update.trip.trip_id', 
                                                'trip_update.trip.start_DateTimeUTC', 
                                                'stop_sequence'])['trip_update.timestamp'].diff().ge(240).fillna(0)*1

    df_NoROT4['Bad_Flag1'] = df_NoROT4.groupby(['trip_update.trip.route_id', 
                                                'trip_update.trip.trip_id', 
                                                'trip_update.trip.start_DateTimeUTC', 
                                                'stop_sequence'])['Bad_Flag0'].cumsum()
    return(df_NoROT4)

#################################################
## Clean Duplicate Data
def Df_Remove_Duplicate(df_Dup):
#     df_Dup.sort_values(by=['trip_update.trip.route_id',
#                            'trip_update.trip.trip_id',
#                            'trip_update.trip.start_DateTimeUTC',
#                            'stop_sequence',
#                            'trip_update.timestamp'
#                           ], inplace=True)
    df_Dup2 = df_Dup[df_Dup['Bad_Flag1'] == 0]
    ## Drop Columns
    df_Dup2 = df_Dup2.drop(columns=['Bad_Flag0','Bad_Flag1'])
    df_Unique = df_Dup2.drop_duplicates(subset=['trip_update.trip.route_id',
                                                'trip_update.trip.trip_id',
                                                'trip_update.trip.start_DateTimeUTC',
                                                'stop_sequence'
                                               ], keep='last')
    return(df_Unique)

#################################################
## Static Directory Path
FileStaticZip = 'complete_gtfs_scheduled_data_' + FileIdStatic + '.zip'
DirStaticZip = DataDir + '/' + FldRawStatic + '/' + FileStaticZip

ZipStatic = ZipFile(DirStaticZip)
df_StTimes = pd.read_csv(ZipStatic.open('stop_times.txt'),
                         dtype={'trip_id':'str','arrival_time':'str','departure_time':'str','stop_id':'str',
                                'stop_sequence':'Int64','stop_headsign':'str','pickup_type':'int','drop_off_type':'int',
                                'shape_dist_traveled':'float','timepoint':'int','stop_note':'str'},
                        )

#################################################

### FOR ARTEMIS: Combine Complete Raw CSV Files

## Record Start Time
tStart = datetime.now()
print('PROCESSING DATA FOR', FileTP, "...")
print('Time Start:', tStart.isoformat(' ', 'seconds'))
       
## Define File Path
PathTransTUrtNoROT = DirTransTU + '/' + GTFS_TU_Prefix + '_' + FileTP + '_NoROT.csv'
PathTransTUrtCln = DirClnTU + '/' + GTFS_TU_Prefix + '_' + FileTP + '_Cln.csv'

## Check if file exists. Remove if exist.
if os.path.exists(PathTransTUrtNoROT):
    os.remove(PathTransTUrtNoROT)
if os.path.exists(PathTransTUrtCln):
    os.remove(PathTransTUrtCln)

## Filter Route and Concatenate All CSV Files in Folder (add new column with Filename as trace)
all_files = glob.glob(os.path.join(DirRawCSVtu, GTFS_TU_Prefix + '*.csv'))

iFile = 0
df_Con = []

for f in all_files:

    ## Count File
    iFile = iFile + 1

    ## Get FullFileName from Path
    FullFileName = f.split('/')[-1]     ## FOR LINUX COMPUTERS
##    FullFileName = f.split('\\')[-1]    ## FOR WINDOWS COMPUTERS
    ## FileName exclude Extension
    FNexExt = os.path.splitext(FullFileName)[0]

    if iFile == 1:
        ## Call function to read raw TU CSV files
        df_Con = Read_CSV_Raw_TU(f)
        ## Call function remove ROT records
        df_Con_NoROT1 = Df_Remove_ROT(df_Con)
    else:
        ## Call function to read raw TU CSV files
        df_X = Read_CSV_Raw_TU(f)
        ## Call function remove ROT records
        df_X_NoROT1 = Df_Remove_ROT(df_X)

        ## Combine records from df_Con_Flt and df_X_Flt
        df_Con_NoROT1 = pd.concat([df_Con_NoROT1, df_X_NoROT1], ignore_index=True)

## Calculate Scheduled ArrivalTime
df_Con_NoROT1 = Df_SchArrTime(df_Con_NoROT1)

## Get Stop Sequence from GTFS Static
df_Con_NoROT2 = Df_GetStaticStopSeq(df_Con_NoROT1)

## Get shape_dist_traveled from GTFS Static
df_Con_NoROT3 = Df_GetStaticDist(df_Con_NoROT2)

## Flag Bad Observations
df_Con_NoROT4 = Df_FlagBad(df_Con_NoROT3)

## Export concatenated files to CSV
df_Con_NoROT = df_Con_NoROT4
df_Con_NoROT.to_csv(PathTransTUrtNoROT, index=False)

## Clean Duplicate Data
df_ConTU_Cln = Df_Remove_Duplicate(df_Con_NoROT)
## Export Cleaned Data to CSV
df_ConTU_Cln.to_csv(PathTransTUrtCln, index=False)

## Record End Time
tEnd = datetime.now()
print(iFile, 'Files Processed:', tEnd.isoformat(' ', 'seconds') + '; Time Spent:', tEnd-tStart)
print('After ROT Removed:', df_Con_NoROT.shape)
print('Cleaned:', df_ConTU_Cln.shape)
print('COMPLETED ON', datetime.now())
print('Transformed file saved in:', PathTransTUrtNoROT)
print('Cleaned file saved in:', PathTransTUrtCln)
