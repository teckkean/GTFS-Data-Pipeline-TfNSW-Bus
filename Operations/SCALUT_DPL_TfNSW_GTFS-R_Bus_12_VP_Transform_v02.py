#!/usr/bin/env python
'''
Smart City Applications in Land Use and Transport (SCALUT)
TfNSW GTFS-R Bus Vehicle Positions

1.2 Transform .CSV Files

$ python SCALUT_DPL_TfNSW_GTFS-R_Bus_12_VP_Transform_v02.py "DataDir" "FileTP"
EXAMPLE: python SCALUT_DPL_TfNSW_GTFS-R_Bus_12_VP_Transform_v02.py "/scratch/RDS-FEI-SCALUT-RW/TfNSW_GTFS_Buses" "2020m10d01" 
'''

### Housekeeping: Import Libraries/Packages
import sys
import os
from datetime import datetime
import pandas as pd
import glob
import time
import geopandas as gpd
import warnings

### Specify Project Directory and Folders and Define Variables

## Specify the folder that stores the .PB.GZ files to be processed
FileTP = sys.argv[2]

## Specify the GTFS-R file prefix
GTFS_VP_Prefix = 'GTFS_VP'

## Specify the main directory that stores input and output folders
DataDir = sys.argv[1]

## Specify the main folders that store input and output data
FldRawCSVvp = '11_CSV_Raw_VP'
FldTransVP = '12_CSV_Transformed_VP'
FldVP_Shp = '22_SHP_VP_GIS'

## Directory Path
DirRawCSVvp = DataDir + '/' + FldRawCSVvp + '/' + FileTP
if not os.path.exists(DirRawCSVvp):
    os.makedirs(DirRawCSVvp)

DirTransVP = DataDir + '/' + FldTransVP 
if not os.path.exists(DirTransVP):
    os.makedirs(DirTransVP)

DirVP_Shp = DataDir + '/' + FldVP_Shp + '/' + FileTP
if not os.path.exists(DirVP_Shp):
    os.makedirs(DirVP_Shp)

#################################################
#### Define Functions
#################################################
## Read Raw VP CSV
def Read_CSV_Raw_VP(f):
    df_CSV_Raw_VP = pd.read_csv(f, sep=',', dtype={'id': 'str',
                                                   'vehicle.trip.trip_id': 'str',
                                                   'vehicle.trip.start_time': 'str',
                                                   'vehicle.trip.start_date': 'str',
                                                   'vehicle.trip.schedule_relationship': 'str',
                                                   'vehicle.trip.route_id': 'str',
                                                   'vehicle.position.latitude': 'float',
                                                   'vehicle.position.longitude': 'float',
                                                   'vehicle.position.bearing': 'float',
                                                   'vehicle.position.speed': 'float',
                                                   'vehicle.timestamp': 'Int64',
                                                   'vehicle.congestion_level': 'str',
                                                   'vehicle.vehicle.id': 'str',
                                                   #                                                    'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].air_conditioned':'str',
                                                   #                                                    'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].wheelchair_accessible':'Int64',
                                                   #                                                    'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].vehicle_model':'str',
                                                   #                                                    'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].performing_prior_trip':'str',
                                                   #                                                    'vehicle.vehicle.[transit_realtime.tfnsw_vehicle_descriptor].special_vehicle_attributes':'Int64',
                                                   'vehicle.occupancy_status': 'str',
                                                   'VPheaderTS': 'str',
                                                   'vehicle.timestampUTC': 'str',
                                                   'vehicle.trip.start_DateTimeUTC': 'str'},
                                #                                 parse_dates=['vehicle.trip.start_DateTimeUTC']
                                )
    return (df_CSV_Raw_VP)


### FOR ARTEMIS: Combine Complete Raw CSV Files

## Record Start Time
tStart = datetime.now()
print('PROCESSING DATA FOR', FileTP, "...")
print('Time Start:', tStart.isoformat(' ', 'seconds'))

## Define File Path
PathTransVP = DirTransVP + '/' + GTFS_VP_Prefix + '_' + FileTP + '.csv'
PathTransVPshp = DirVP_Shp + '/' + GTFS_VP_Prefix + '_' + FileTP + '.shp'

## Check if Transformed VP file exists. Remove if exist.
if os.path.exists(PathTransVP):
    os.remove(PathTransVP)
if os.path.exists(PathTransVPshp):
    os.remove(PathTransVPshp)

## concatenate All CSV Files in Folder (add new column with Filename as trace)
all_files = glob.glob(os.path.join(DirRawCSVvp, GTFS_VP_Prefix + '*.csv'))

iFile = 0
df_Con = []

for f in all_files:

    ## Count File
    iFile = iFile + 1

    ## Get FullFileName from Path
    FullFileName = f.split('/')[-1]
## FOR WINDOWS COMPUTERS:    FullFileName = f.split('\\')[-1]

    ## FileName exclude Extension
    FNexExt = os.path.splitext(FullFileName)[0]

    if iFile == 1:
        df_ConVP = Read_CSV_Raw_VP(f)
    else:
        df_X = Read_CSV_Raw_VP(f)

        df_ConVP = pd.concat([df_ConVP, df_X], ignore_index=True)

## Export concatenated file to CSV
df_ConVP.to_csv(PathTransVP, index=False)
print(df_ConVP.shape)

## Convert df_ConVP_Flt into Spatial Information
gdf_ConVP = gpd.GeoDataFrame(df_ConVP,
                             geometry=gpd.points_from_xy(df_ConVP['vehicle.position.longitude'],
                                                         df_ConVP['vehicle.position.latitude']),
                             crs='EPSG:4326')
## Export to SHP File
warnings.filterwarnings("ignore")
gdf_ConVP.to_file(PathTransVPshp)
warnings.resetwarnings()

## Record End Time
tEnd = datetime.now()
print(iFile, 'Files Processed:', tEnd.isoformat(' ', 'seconds') + '; Time Spent:', tEnd - tStart)
print('Transformed VP file saved in:', PathTransVP)
print('GIS file saved in:', PathTransVPshp)
