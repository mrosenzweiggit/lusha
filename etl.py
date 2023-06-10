import pandas as pd


def extract(source, file_type, skip_rows=0):
    """
    This function connect to source and extract the data
    :param source: filepath of the source
    :param file_type: csv, json, other
    :param skip_rows: number of rows to skip
    :return: pandas dataframe
    """
    df = pd.DataFrame()

    if file_type == 'csv':
        # df = pd.read_csv(source, skiprows=skip_rows, infer_datetime_format=True)
        df = pd.read_csv(source, skiprows=skip_rows)
    elif file_type == 'json':
        df = pd.read_json(source)
    elif file_type == 'parquet':
        df = pd.read_parquet(source)
    else:
        print("[INFO] No data source founded")

    # pd.to_datetime(df, format="%d-%m-%y")
    return df


def merge_df(df_l, df_r, how, left_on, right_on):
    """
    This function merge 2 dataframes
    :param right_on: column level names to join on in the right df
    :param left_on: column level names to join on in the left df
    :param on:
    :param df_l: left pandas dataframe
    :param df_r: right pandas dataframe
    :param how: type of merge (inner, left, right, outer, cross)
    :return:
    """
    df_result = df_l.merge(df_r, how=how, left_on=left_on, right_on=right_on, suffixes=('_left', '_right'))

    return df_result
