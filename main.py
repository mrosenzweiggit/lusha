import pandas as pd
import os
import etl
import analytics
import csv
import pyarrow.parquet as pq


def main():

    # Get the path to the folder containing the Parquet files
    folder_path = './sources/data_part1'
    sub_folder = 'yellow_taxi_jan_25_2018'

    # Read the Parquet and csv files into DataFrames
    df_trips_1 = etl.extract(os.path.join(folder_path, sub_folder, 'part-00000-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet'), "parquet")
    df_trips_2 = etl.extract(os.path.join(folder_path, sub_folder, 'part-00004-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet'), "parquet")
    df_taxi_zones = etl.extract(os.path.join(folder_path, 'taxi_zones.csv'), "csv")

    # Enrichment data
    df_trips_2['is_long_ride'] = (df_trips_2['trip_distance'] > 10).astype('bool')
    df_trips_2['is_taxi'] = (df_trips_2['passenger_count'] < 4).astype('bool')
    df_trips_2['hour_pickup_id'] = df_trips_2['tpep_pickup_datetime'].dt.hour
    df_trips_2['hour_dropoff_id'] = df_trips_2['tpep_dropoff_datetime'].dt.hour

    df_trips_1.to_csv('./output/df1.csv', index=False)
    df_trips_2.to_csv('./output/df2.csv', index=False)
    df_taxi_zones.to_csv('./output/df_taxi_zones.csv', index=False)

    print('Borough with the most pickups and dropoffs')
    print(analytics.get_borough_with_most_picukps_dropoffs(df_trips_2, df_taxi_zones))

    print('Peak hour for Taxi:')
    print(analytics.get_peak_hours_for_taxi(df_trips_2))

    print('Peak Hour for long/short trips')
    print(analytics.get_peak_hours_for_long_short_trips(df_trips_2))

    print('Payment type by long/short trips')
    print(analytics.get_how_people_paying_for_the_ride(df_trips_2))


if __name__ == '__main__':
    print('Starting ETL')
    main()
    print('finished ETL')
