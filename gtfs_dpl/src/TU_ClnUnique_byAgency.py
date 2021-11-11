#!/usr/bin/env python
'''
Smart City Applications in Land Use and Transport (SCALUT)
TfNSW GTFS-R Bus Trip Update 

1.3 Prepare Cleaned Unique Datasets

$ python SCALUT_DPL_TfNSW_GTFS-R_Bus_13_TU_ClnUnique_v02_byAgency.py "DataDir" "FileTP" "FileIdStatic" "DaysInMonth" "Flt_Agency"
EXAMPLE: SCALUT_DPL_TfNSW_GTFS-R_Bus_13_TU_ClnUnique_v02_byAgency.py "/scratch/RDS-FEI-SCALUT-RW/TfNSW_GTFS_Buses" "2020m10" "20201001191000" "31" "Premier Illawarra"
'''

### Housekeeping: Import Libraries/Packages
import sys
import os
from datetime import datetime
import pandas as pd
import time
from zipfile import ZipFile

### Specify Project Directory and Folders and Define Variables

## Specifiy the main directory that stores input and output folders
DataDir = sys.argv[1]

## Specify the folder that stores the .PB.GZ files to be processed
FileTP = sys.argv[2]
DaysInMonth = int(sys.argv[4])

## Specify the GTFS-R file prefix
GTFS_TU_Prefix = 'GTFS_TU'

## Specifiy the main folders that stores GTFS Static data
FldRawStatic = '10_Raw_Static'
FileIdStatic = sys.argv[3]

## Specifiy the main folders that stores input and output data
FldClnTU = '13_CSV_Cleaned_Unique_TU'
FldClnTU_Agency = '13_CSV_Cleaned_Unique_TU_byAgency'

## Specify the filename and location for the Routes List 
Flt_Agency = sys.argv[5]

## Directory Path
DirClnTU = DataDir + '/' + FldClnTU + '/' + FileTP
if not os.path.exists(DirClnTU):
    os.makedirs(DirClnTU)
    
DirClnTU_Agency = DataDir + '/' + FldClnTU_Agency + '/' + FileTP
if not os.path.exists(DirClnTU_Agency):
    os.makedirs(DirClnTU_Agency)

#################################################
#### Define Functions
#################################################
## Read Cleaned Unique TU CSV
def Read_CSV_Cln_TU(f):
    df_CSV_Cln_TU = pd.read_csv(f, sep=',', dtype={'id':'str',
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
                                                   'trip_update.trip.start_DateTimeUTC':'str',
                                                   'Rt.Scheduled_Arrival.Time':'str',
                                                   'Rt.Scheduled_Arrival.TimeUTC':'str',
                                                   'trip_update.trip.trip_id2':'str',
                                                   'Rt.Scheduled_Arrival.TimeUTC2':'str',
                                                   'stop_sequence':'Int64',
                                                   'shape_dist_traveled':'float'
                                                  }, 
                                parse_dates=['trip_update.stop_time_update.arrival.timeUTC','trip_update.trip.start_DateTimeUTC']
                               )
    return(df_CSV_Cln_TU)

############################################################################
## Fill Empty Stop Sequence Using Existing Data AND Flag Bad Observations ##
def Fill_Empty_StopSeq3(df, df_S):
    df = df.astype({'stop_sequence':'float'})
    df['stop_sequence'] = df.groupby(['trip_update.trip.route_id', 
                                      'trip_update.trip.trip_id', 
                                      'trip_update.trip.start_DateTimeUTC',
                                      'trip_update.stop_time_update.stop_id'
                                     ])['stop_sequence'].apply(lambda x:x.fillna(x.mean()))
    df['stop_sequence'] = df['stop_sequence'].round(0).astype('Int64')

    ## Get shape_dist_traveled from GTFS Static
    df1 = pd.merge(df, 
                   df_S[['trip_id','stop_id','stop_sequence','shape_dist_traveled']],
                   how='left',
                   suffixes=('','_S'),
                   left_on=['trip_update.trip.trip_id2','trip_update.stop_time_update.stop_id','stop_sequence'],
                   right_on=['trip_id','stop_id','stop_sequence'])
    df1['shape_dist_traveled'].fillna(df1['shape_dist_traveled_S'], inplace=True)
    ## Drop Columns
    df2 = df1.drop(columns=['trip_id','stop_id','shape_dist_traveled_S'])
    ## Flag Bad Observations
    df2.sort_values(by=['trip_update.trip.route_id', 
                        'trip_update.trip.trip_id',
                        'trip_update.trip.start_DateTimeUTC',
                        'stop_sequence',
                        'trip_update.timestamp'
                       ], inplace=True)

    ## Bad when 300s gap (from TP to TP)
    df2['Bad_Flag0'] = df2.groupby(['trip_update.trip.route_id', 
                                    'trip_update.trip.trip_id', 
                                    'trip_update.trip.start_DateTimeUTC', 
                                    'stop_sequence'])['trip_update.timestamp'].diff().ge(300).fillna(0)*1

    df2['Bad_Flag1'] = df2.groupby(['trip_update.trip.route_id', 
                                    'trip_update.trip.trip_id', 
                                    'trip_update.trip.start_DateTimeUTC', 
                                    'stop_sequence'])['Bad_Flag0'].cumsum()

    return(df2)

#################################################
## Clean Duplicate Data
def Df_Remove_Duplicate(df_Dup):
    df_Dup2 = df_Dup[df_Dup['Bad_Flag1'] == 0]
    ## Drop Columns
    df_Dup2 = df_Dup2.drop(columns=['Bad_Flag0','Bad_Flag1'])
    df_Unique = df_Dup2.drop_duplicates(subset=['trip_update.trip.route_id',
                                                'trip_update.trip.trip_id',
                                                'trip_update.trip.start_DateTimeUTC',
                                                'stop_sequence'
                                               ], keep='last')
    return(df_Unique)

#############################################
### Get Information from GTFS Static (Routes List by Agency)
#############################################
## Static Directory Path
FileStaticZip = 'complete_gtfs_scheduled_data_' + FileIdStatic + '.zip'
DirStaticZip = DataDir + '/' + FldRawStatic + '/' + FileStaticZip

ZipStatic = ZipFile(DirStaticZip)
df_StTimes = pd.read_csv(ZipStatic.open('stop_times.txt'),
                         dtype={'trip_id':'str','arrival_time':'str','departure_time':'str','stop_id':'str',
                                'stop_sequence':'Int32','stop_headsign':'str','pickup_type':'int','drop_off_type':'int',
                                'shape_dist_traveled':'float','timepoint':'int','stop_note':'str'},
                        )
# df_StTimes.head(2)

#############################################
## Get List of Routes based on Agency Name ##
df_Agency = pd.read_csv(ZipStatic.open('agency.txt'),dtype='unicode')
df_Routes = pd.read_csv(ZipStatic.open('routes.txt'),dtype='unicode')
df_Routes['RT_route_id'] = df_Routes['agency_id'] + '_' + df_Routes['route_short_name']

df_RoutesAgency = pd.merge(df_Routes,
                           df_Agency[['agency_id','agency_name']],
                           how='left',
                           suffixes=('','_Ag'),
                           on=['agency_id'])

df_RoutesAgency_Flt = df_RoutesAgency[df_RoutesAgency['agency_name'].isin([Flt_Agency])]

List_AgencyRtRoutes = df_RoutesAgency_Flt['RT_route_id']

df_RoutesAgency_FltRT = df_RoutesAgency_Flt[df_RoutesAgency_Flt['route_type'] == '700']

List_AgencyRtRoutes_700 = df_RoutesAgency_FltRT['RT_route_id']

#############################################
### FOR ARTEMIS: Combine Cleaned Datasets from TP1-TP6 to Daily
### and Create Daily Datasets by Agency

## Record Start Time
tStart = datetime.now()
print('PROCESSING DATA FOR', FileTP, "...")
print('Time Start:', tStart.isoformat(' ', 'seconds'))
print('')

for iDay in range(1, DaysInMonth+1):

    FileId = FileTP + 'd' + (str(iDay).zfill(2))

    ## Define Output File Path
    PathTransTUrtCln = DirClnTU + '/' + GTFS_TU_Prefix + '_' + FileId + '_Cln.csv'

    ## Check if file exists. Remove if exist.
    if os.path.exists(PathTransTUrtCln):
        os.remove(PathTransTUrtCln)    

    df_Con = []
    for iTP in range(1, 7):

        ## Define Input File Path
        PathClnTP = DirClnTU + '/' + GTFS_TU_Prefix + '_' + FileId + 'tp' + str(iTP) + '_Cln.csv'    

        if iTP == 1:
            ## Call function to read Cln TU CSV files
            df_Con = Read_CSV_Cln_TU(PathClnTP)
            print(FileId + 'TP' + str(iTP), df_Con.shape)
        else:
            ## Call function to read Cln TU CSV files
            df_X = Read_CSV_Cln_TU(PathClnTP)
            print(FileId + 'TP' + str(iTP), df_X.shape)

            ## Combine records from df_Con_Flt and df_X_Flt
            df_Con = pd.concat([df_Con, df_X], ignore_index=True)

    ############################################
    ## Daily Cleaned Unique Dataset by Agency ##

    ## FOR LIST OF ROUTES BY AGENCY
    Agency = Flt_Agency
    Flt_Routes_Rt = List_AgencyRtRoutes_700

    ## Define Output File Path by Agency and RT700
    PathTransTUrtClnAgency = DirClnTU_Agency + '/' + GTFS_TU_Prefix + '_' + FileId + "_" + Agency + '_700_Cln.csv'

    ## Check if file exists. Remove if exist.
    if os.path.exists(PathTransTUrtClnAgency):
        os.remove(PathTransTUrtClnAgency)

    ## Grab the records associated with the Agency
    df_Con_Flt = df_Con[df_Con['trip_update.trip.route_id'].isin(Flt_Routes_Rt)]

    ## Fill Empty Stop Sequence etc Using Existing Data
    df_Con_Flt1 = Fill_Empty_StopSeq3(df_Con_Flt, df_StTimes)

    ## Clean Duplicate Data
    df_Con_Cln_Flt = Df_Remove_Duplicate(df_Con_Flt1)

    ## Export Agency Daily Cleaned Data to CSV
    df_Con_Cln_Flt.to_csv(PathTransTUrtClnAgency, index=False)

    print('Combined Dataset:', FileId, df_Con.shape)
    print('Combined Dataset by Agency:', FileId, df_Con_Flt.shape)
    print('Combined Unique Dataset by Agency RT700:', Agency, FileId, df_Con_Cln_Flt.shape)
    print('')

## Record End Time
tEnd = datetime.now()
print(iDay, 'Days Processed:', tEnd.isoformat(' ', 'seconds') + '; Time Spent:', tEnd-tStart)
print('COMPLETED ON', datetime.now())

#############################################
### FOR ARTEMIS: Combine Datasets by Agency from Daily to Monthly

## Record Start Time
tStart = datetime.now()
print('PROCESSING DATA FOR', FileTP, "...")
print('Time Start:', tStart.isoformat(' ', 'seconds'))

## Define Output File Path
PathTransTUrtClnAgencyMonth = DirClnTU_Agency + '/' + GTFS_TU_Prefix + '_' + FileTP + '_' + Agency + '_700_Cln.csv'

## Check if file exists. Remove if exist.
if os.path.exists(PathTransTUrtClnAgencyMonth):
    os.remove(PathTransTUrtClnAgencyMonth)

df_ConAgency = []

for iDay in range(1, DaysInMonth+1):

    ## Define Input File Path
    FileId = FileTP + 'd' + (str(iDay).zfill(2))
    PathTransTUrtClnAgency = DirClnTU_Agency + '/' + GTFS_TU_Prefix + '_' + FileId + "_" + Agency + '_700_Cln.csv'

    if iDay == 1:
        ## Call function to read Cln TU CSV files
        df_ConAgency = Read_CSV_Cln_TU(PathTransTUrtClnAgency)
        print('Day' + str(iDay), df_ConAgency.shape)
    else:
        ## Call function to read Cln TU CSV files
        df_XAgency = Read_CSV_Cln_TU(PathTransTUrtClnAgency)
        print('Day' + str(iDay), df_XAgency.shape)

        ## Combine records from df_Con_Flt and df_X_Flt
        df_ConAgency = pd.concat([df_ConAgency, df_XAgency], ignore_index=True)

        
## Fill Empty Stop Sequence etc Using Existing Data
df_ConAgency1 = Fill_Empty_StopSeq3(df_ConAgency, df_StTimes)

## Clean Duplicate Data
df_ConTU_ClnAgency = Df_Remove_Duplicate(df_ConAgency1)

## Export Agency Monthly Cleaned Data to CSV
df_ConTU_ClnAgency.to_csv(PathTransTUrtClnAgencyMonth, index=False)

## Record End Time
tEnd = datetime.now()
print('TP' + str(iTP), 'Files Processed:', tEnd.isoformat(' ', 'seconds') + '; Time Spent:', tEnd-tStart)
print('Combined Dataset:', df_ConAgency.shape)
print('Combined Unique Dataset:', df_ConTU_ClnAgency.shape)
print('COMPLETED ON', datetime.now())