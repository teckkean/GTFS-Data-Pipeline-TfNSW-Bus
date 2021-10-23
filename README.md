[![DOI](https://zenodo.org/badge/383683218.svg)](https://zenodo.org/badge/latestdoi/383683218)
# GTFS Data Pipeline for TfNSW Bus Datasets
![Pipeline](GTFS_TfNSW_Bus_Data_Pipeline_v211023.png)


## Table of Contents
* [Introduction](#introduction)
* [Data Availability Statement](#data-availability-statement)
* [Data Pipeline Directory Structure](#data-pipeline-directory-structure)
* [GTFS Static](#gtfs-static)
* [GTFS Realtime Trip Update](#gtfs-realtime-trip-update)
  - Convert Trip Update .PB file to CSV
* [GTFS Realtime Vehicle Position](#gtfs-realtime-vehicle-position)
  - Convert Vehicle Positions .PB file to CSV
* [GTFS Realtime Service Alert](#gtfs-realtime-service-alert)


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
* GTFS Realtime Datasets: 
  * Trip Update v1 - https://opendata.transport.nsw.gov.au/dataset/public-transport-realtime-trip-update
  * Vehicle Position v1 - https://opendata.transport.nsw.gov.au/dataset/public-transport-realtime-vehicle-positions

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

## GTFS Static

## GTFS Realtime Trip Update

## GTFS Realtime Vehicle Position

## GTFS Realtime Service Alert
Out of scope




