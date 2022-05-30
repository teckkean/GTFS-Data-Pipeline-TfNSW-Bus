import pandas as pd
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

    ## Fill Empty Stop Sequence Using Existing Data
    df_NoROT2 = df_NoROT2.astype({'stop_sequence':'float'})
    df_NoROT2['stop_sequence'] = df_NoROT2.groupby(['trip_update.trip.route_id', 
                                                    'trip_update.trip.trip_id', 
                                                    'trip_update.trip.start_DateTimeUTC',
                                                    'trip_update.stop_time_update.stop_id'
                                                   ])['stop_sequence'].apply(lambda x:x.fillna(x.mean()))
    df_NoROT2['stop_sequence'] = df_NoROT2['stop_sequence'].round(0).astype('Int64')

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