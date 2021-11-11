#!/usr/bin/env python
'''
Smart City Applications in Land Use and Transport (SCALUT)
TfNSW GTFS-R Bus Vehicle Positions

1.2A Transform .CSV Files by Agency (Daily to Monthly)

$ python SCALUT_DPL_TfNSW_GTFS-R_Bus_12A_VP_Transform_v01_byAgency.py "DataDir" "FileTP" "FileIdStatic" "DaysInMonth" "Flt_Agency"
EXAMPLE: python SCALUT_DPL_TfNSW_GTFS-R_Bus_12A_VP_Transform_v01_byAgency.py "/project/RDS-FEI-SCALUT-RW/TfNSW_GTFS_Buses" "2020m09" "20200901190900" "30" "Premier Illawarra"
'''

### Housekeeping: Import Libraries/Packages
import sys
import os
from datetime import datetime
import pandas as pd
import time
from zipfile import ZipFile
import geopandas as gpd
import warnings

### Specify Project Directory and Folders and Define Variables

## Specify the folder that stores the .PB.GZ files to be processed
FileTP = sys.argv[2]
DaysInMonth = int(sys.argv[4])

# ## Specify the GTFS-R file prefix
GTFS_VP_Prefix = 'GTFS_VP'

## Specify the main directory that stores input and output folders
DataDir = sys.argv[1]

## Specifiy the main folders that stores GTFS Static data
FldRawStatic = '10_Raw_Static'
FileIdStatic = sys.argv[3]

## Specify the main folders that store input and output data
FldTransVP = '12_CSV_Transformed_VP'
FldTransVP_Agency = '12_CSV_Transformed_VP_byAgency'
FldVP_Shp = '22_SHP_VP_GIS'
FldVP_Shp_Agency = '22_SHP_VP_GIS_byAgency'

## Specify the filename and location for the Routes List 
Flt_Agency = sys.argv[5]

## Directory Path
DirTransVP = DataDir + '/' + FldTransVP + '/' + FileTP
if not os.path.exists(DirTransVP):
    os.makedirs(DirTransVP)

DirVP_Shp = DataDir + '/' + FldVP_Shp + '/' + FileTP
if not os.path.exists(DirVP_Shp):
    os.makedirs(DirVP_Shp)

DirTransVP_Agency = DataDir + '/' + FldTransVP_Agency + '/' + FileTP
if not os.path.exists(DirTransVP_Agency):
    os.makedirs(DirTransVP_Agency)

DirVP_Shp_Agency = DataDir + '/' + FldVP_Shp_Agency + '/' + FileTP
if not os.path.exists(DirVP_Shp_Agency):
    os.makedirs(DirVP_Shp_Agency)

#################################################
#### Define Functions
#################################################
## Read Transformed VP CSV
def Read_CSV_TransVP(f_TransVP):
    df_TransVP = pd.read_csv(f_TransVP, 
                             sep=',', 
                             dtype={'id':'str',
                                    'vehicle.trip.trip_id':'str',
                                    'vehicle.trip.start_time':'str',
                                    'vehicle.trip.start_date':'str',
                                    'vehicle.trip.schedule_relationship':'str',
                                    'vehicle.trip.route_id':'str',
                                    'vehicle.position.latitude':'float',
                                    'vehicle.position.longitude':'float',
                                    'vehicle.position.bearing':'float',
                                    'vehicle.position.speed':'float',
                                    'vehicle.timestamp':'Int64',
                                    'vehicle.congestion_level':'str',
                                    'vehicle.vehicle.id':'str',
#                                     'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].air_conditioned':'str', 
#                                     'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].wheelchair_accessible':'Int64',
#                                     'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].vehicle_model':'str',
#                                     'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].performing_prior_trip':'str',
#                                     'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].special_vehicle_attributes':'Int64',
                                    'vehicle.occupancy_status':'str',
                                    'VPheaderTS':'str',
                                    'vehicle.timestampUTC':'str',
                                    'vehicle.trip.start_DateTimeUTC':'str'},
#                              parse_dates=['vehicle.timestampUTC','vehicle.trip.start_DateTimeUTC']
                            )
    return(df_TransVP)

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
# df_RoutesAgency_Flt.head(1)

List_AgencyRtRoutes = df_RoutesAgency_Flt['RT_route_id']

#############################################
### FOR ARTEMIS: Create Daily Datasets by Agency and Combine into Monthly

## Record Start Time
tStart = datetime.now()
print('PROCESSING DATA FOR', FileTP, "...")
print('Time Start:', tStart.isoformat(' ', 'seconds'))
print('')

####################################
## Filter Daily Dataset by Agency ##

## FOR LIST OF ROUTES BY AGENCY
Agency = Flt_Agency
Flt_Routes_Rt = List_AgencyRtRoutes

## Define Output File Path by Agency
PathTransVPAgency = DirTransVP_Agency + '/' + GTFS_VP_Prefix + '_' + FileTP + "_" + Agency + '.csv'
PathTransVPshpAgency = DirVP_Shp_Agency + '/' + GTFS_VP_Prefix + '_' + FileTP + "_" + Agency + '.shp'

## Check if file exists. Remove if exist.
if os.path.exists(PathTransVPAgency):
    os.remove(PathTransVPAgency)
if os.path.exists(PathTransVPshpAgency):
    os.remove(PathTransVPshpAgency)

# df_ConVP = []
for iDay in range(1, DaysInMonth+1):

    ## Define Input File Path
    PathTransVP = DirTransVP + '/' + GTFS_VP_Prefix + '_' + FileTP + 'd' + (str(iDay).zfill(2)) + '.csv'

    if iDay == 1:
        ## Call function to read Cln TU CSV files
        df_ConVP = Read_CSV_TransVP(PathTransVP)
        print(FileTP + 'd' + (str(iDay).zfill(2)), df_ConVP.shape)
        ## Filter by Agency
        df_ConVP_Flt = df_ConVP[df_ConVP['vehicle.trip.route_id'].isin(Flt_Routes_Rt)]
        print(FileTP + 'd' + (str(iDay).zfill(2)) + ' for ' + Agency, df_ConVP_Flt.shape)
    else:
        df_X = Read_CSV_TransVP(PathTransVP)
        print(FileTP + 'd' + (str(iDay).zfill(2)), df_X.shape)
        ## Filter by Agency
        df_X_Flt = df_X[df_X['vehicle.trip.route_id'].isin(Flt_Routes_Rt)]
        print(FileTP + 'd' + (str(iDay).zfill(2)) + ' for ' + Agency, df_X_Flt.shape)
        
        df_ConVP_Flt = pd.concat([df_ConVP_Flt, df_X_Flt], ignore_index=True)

## Export concatenated file to CSV
df_ConVP_Flt.to_csv(PathTransVPAgency, index=False)

## Convert df_ConVP_Flt into Spatial Information
gdf_ConVP_Flt = gpd.GeoDataFrame(df_ConVP_Flt, 
                                 geometry=gpd.points_from_xy(df_ConVP_Flt['vehicle.position.longitude'], 
                                                             df_ConVP_Flt['vehicle.position.latitude']),
                                 crs='EPSG:4326')
## Export to SHP File
warnings.filterwarnings("ignore")
gdf_ConVP_Flt.to_file(PathTransVPshpAgency)
warnings.resetwarnings()

## Record End Time
tEnd = datetime.now()
print('')
print(FileTP + ' for ' + Agency, df_ConVP_Flt.shape)
print(iDay, 'Files Processed:', tEnd.isoformat(' ', 'seconds') + '; Time Spent:', tEnd-tStart)
print('COMPLETED ON', datetime.now())
