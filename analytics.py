import pandas as pd

def get_borough_with_most_picukps_dropoffs(df, dim_df):
    """
    This function returns the borough with the most pickups and dropoffs.
    :param df: A pandas dataframe.
    :return: The borough with the most pickups and dropoffs.
    """

    # Group the dataframe by borough and count the number of pickups and dropoffs in each borough.
    df_boroughs = df.groupby(['PULocationID'])['PULocationID'].count() + df.groupby(['DOLocationID'])['DOLocationID'].count()

    df_boroughs = df_boroughs.to_frame()
    df_boroughs.columns = ['quantity']
    df_boroughs.index.name = 'LocationID'

    df_boroughs = df_boroughs.merge(dim_df, how='left', on='LocationID', suffixes=('_left', '_right'))

    # Sort the dataframe by the number of pickups and dropoffs in descending order.
    df_boroughs = df_boroughs.sort_values(by=['quantity'], ascending=False)

    # Return the borough with the most pickups and dropoffs
    return df_boroughs[['LocationID', 'Borough', 'quantity']].head(1)


def get_peak_hours_for_taxi(df):
    """
    This function returns the peak hours for taxi. Assumption: is taxi if passenger_count < 5
    :param df: A pandas dataframe
    :return: The top 5 peak hours for a taxi.
    """
    # Group the dataframe by borough and count the number of pickups and dropoffs in each borough.
    df_is_taxi = df[df['is_taxi'] == True]
    df_peak_hours_for_taxi = df_is_taxi.groupby(['hour_pickup_id'])['hour_pickup_id'].count()

    df_peak_hours_for_taxi = df_peak_hours_for_taxi.to_frame()
    df_peak_hours_for_taxi.columns = ['quantity']
    df_peak_hours_for_taxi.index.name = 'peak_hour_id'

    # Sort the dataframe by the number of trips by hour in descending order.
    df_peak_hours_for_taxi = df_peak_hours_for_taxi.sort_values( by=['quantity'], ascending=False)

    # Return the borough with the most pickups and dropoffs
    return df_peak_hours_for_taxi.head(5)

def get_peak_hours_for_long_short_trips(df):
    """
    This function returns the peak hours for taxi
    :param df: A pandas dataframe
    :return: The top 5 peak hours for a taxi
    """
    # Group the dataframe by borough and count the number of pickups and dropoffs in each borough.
    df_peak_hours_for_long_short = df.groupby(['hour_pickup_id'])['hour_pickup_id'].count()

    df_peak_hours_for_long_short = df_peak_hours_for_long_short.to_frame()
    df_peak_hours_for_long_short.columns = ['quantity']
    df_peak_hours_for_long_short.index.name = 'peak_hour_id'

    # Sort the dataframe by the number of trips by hour in descending order.
    df_peak_hours_for_long_short = df_peak_hours_for_long_short.sort_values( by=['quantity'], ascending=False)

    # Return the borough with the most pickups and dropoffs
    return df_peak_hours_for_long_short.head(5)


def get_how_people_paying_for_the_ride(df):
    """
    This function returns the payment distribution by payment type to know how the people pay.
    Assumption: is a long_ride if trip_distance > 10
    :param df: A pandas dataframe.
    :return: a df with the payment type payment distribution by long/short trip.
    """
    # Group the dataframe by borough and count the number of pickups and dropoffs in each borough.
    df_payment_type = df.groupby(['payment_type', 'is_long_ride'])['payment_type'].count()

    df_payment_type = df_payment_type.to_frame()
    df_payment_type.columns = ['quantity']

    # Sort the dataframe by the number of pickups and dropoffs in descending order.
    df_payment_type = df_payment_type.sort_values(by=['payment_type'], ascending=True)

    # Return the borough with the most pickups and dropoffs
    return df_payment_type