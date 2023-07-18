import pandas as pd
from dateutil.relativedelta import relativedelta


def calculate_average_trip_length(df):
    return df['trip_distance'].mean()


def calculate_rolling_average_trip_length(df, window_size):
    return df['trip_distance'].rolling(window=window_size).mean()


def main():
    # As the file downloaded is of April 2023
    input_parquet_file = 'yellow_tripdata_2023-04.parquet'
    target_month = 4
    target_year = 2023

    df = pd.read_parquet(input_parquet_file)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    target_date_start = pd.Timestamp(target_year, target_month, 1)
    target_date_end = target_date_start + relativedelta(months=1) - relativedelta(days=1)
    filtered_df = df[
        (df['tpep_pickup_datetime'] >= target_date_start) & (df['tpep_pickup_datetime'] <= target_date_end)]

    average_trip_length = calculate_average_trip_length(filtered_df)
    print(f"Average trip length for {target_year}-{target_month:02}: {average_trip_length:.2f} miles")

    rolling_average = calculate_rolling_average_trip_length(df, window_size=45)
    print("\n45-Day Rolling Average Trip Length:")
    print(rolling_average)


if __name__ == "__main__":
    main()
