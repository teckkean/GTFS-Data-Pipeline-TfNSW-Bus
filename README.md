[![DOI](https://zenodo.org/badge/383683218.svg)](https://zenodo.org/badge/latestdoi/383683218)
# GTFS Data Pipeline for TfNSW Bus Datasets
![Pipeline](GTFS_TfNSW_Bus_Data_Pipeline_v211023.png)


## Table of Contents
* [Introduction](#introduction)
* [Data Availability Statement](#data-availability-statement)
* [Data Pipeline Directory Structure](#data-pipeline-directory-structure)
* [Data Pipeline Operations](#data-pipeline-operations)
  - 1.1 Convert .PB.GZ to .CSV Files
  - 1.2 Transform .CSV Files
  - 1.2A Transform .CSV Files by Agency (Daily to Monthly)
  - 1.3 Prepare Cleaned Unique Datasets


## Introduction

**PhD Research Project Title:** Smart City Applications in Land Use and Transport (SCALUT)

This is a data pipeline developed as part of the PhD research project, SCALUT, at the University of Sydney's TransportLab. 

The datasets generated using this pipeline has been used to validate the performance of TfNSW's Transit Signal Priority Request via Public Transport Information and Priority System (PTIPS).

The data pipeline is written in Python and has been tested to work on Windows, Linux and Mac using the Version 1 GTFS TfNSW Bus Datasets. 

*Note: A seperate data pipeline is currently being developed and tested to work with a wider collection of GTFS datasets.*


## Data Availability Statement

The datasets generated will be made available to public on the University of Sydney Data Repository. 

On-going static and realtime datasets are available on the Transport for NSW Open Data Hub:
* GTFS Static Datasets: https://opendata.transport.nsw.gov.au/dataset/timetables-complete-gtfs
* GTFS Realtime v1 Datasets: 
  - Trip Update - https://opendata.transport.nsw.gov.au/dataset/public-transport-realtime-trip-update
  - Vehicle Position - https://opendata.transport.nsw.gov.au/dataset/public-transport-realtime-vehicle-positions


## Data Pipeline Directory Structure
```
GTFS_TfNSW_Bus_DataWareHouse
├───10_Raw_PB
│   └───FileTP
├───10_Raw_Static
├───10_TfNSW_Traffic_Lights_Location
├───10_TfNSW_Traffic_Volume_Viewer
├───11_CSV_Raw_TU
│   └───FileTP
├───11_CSV_Raw_VP
│   └───FileTP
├───12_CSV_Transformed_TU
│   └───FileTP
├───12_CSV_Transformed_VP
│   └───FileTP
├───12_CSV_Transformed_VP_byAgency
│   └───FileTP
├───13_CSV_Cleaned_Unique_TU
│   └───FileTP
├───13_CSV_Cleaned_Unique_TU_byAgency
│   └───FileTP
├───21_SA_Static
│   ├───GTFS_Static_StaticId
│   └───TL_Location_StaticId
├───22_CSV_Fu_Nodes_Links
│   └───FileTP
├───22_SHP_Fu_Nodes_Links
│   └───FileTP
├───22_SHP_VP_GIS
│   └───FileTP
└───22_SHP_VP_GIS_byAgency
    └───FileTP
```


## Data Pipeline Operations
**1.1 Convert .PB.GZ to .CSV Files**
```
SCALUT_DPL_TfNSW_GTFS-R_Bus_11_TU_PBtoCSV_v02A.py
SCALUT_DPL_TfNSW_GTFS-R_Bus_11_VP_PBtoCSV_v02A.py
```
**1.2 Transform .CSV Files**
```
SCALUT_DPL_TfNSW_GTFS-R_Bus_12_TU_Transform_v03.py
SCALUT_DPL_TfNSW_GTFS-R_Bus_12_VP_Transform_v02.py
```
**1.2A Transform .CSV Files by Agency (Daily to Monthly)**
```
SCALUT_DPL_TfNSW_GTFS-R_Bus_12A_VP_Transform_v01_byAgency.py
```
**1.3 Prepare Cleaned Unique Datasets**
```
SCALUT_DPL_TfNSW_GTFS-R_Bus_13_TU_ClnUnique_v02_byAgency.py
```
