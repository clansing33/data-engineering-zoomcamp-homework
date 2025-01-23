import argparse
import os
import pandas as pd
from sqlalchemy import create_engine, text
from time import time


import subprocess

def download_file(url, output_path):
    if not os.path.exists(output_path):
        print(f"File not found: {output_path}. Downloading...")
        try:
            subprocess.run(["wget", "-O", output_path, url], check=True)
            print(f"File downloaded successfully: {output_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error downloading file: {e}")
    else:
        print(f"File already exists: {output_path}. Skipping download.")




def main(params):

    # get params values
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    table_name_lookup = "taxi_zones"

    csv_green_trip = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
    path_green_trip = "green_tripdata_2019-10.csv.gz"

    download_file(csv_green_trip, path_green_trip)

    csv_zone_lookup = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    path_zone_lookup = "taxi_zone_lookup.csv"

    download_file(csv_zone_lookup, path_zone_lookup)
    
    chunksize = 100000

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # # drop tables if they exist
    # drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
    # drop_table_lookup_query = f"DROP TABLE IF EXISTS {table_name_lookup}"

    # with engine.connect() as connection:
    #     connection.execute(text(drop_table_query))
    #     connection.execute(text(drop_table_lookup_query))

    # lookup table
    df_lu = pd.read_csv("taxi_zone_lookup.csv")
    df_lu.to_sql(name='taxi_zones', con=engine, if_exists='replace')



    # extract    
    df_iter = pd.read_csv(path_green_trip, iterator=True, chunksize=chunksize)

    df = next(df_iter)

    # transform

    # green
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # create empty table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    # insert data

    # load
    #1st chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # the rest of the data by chunk
    while True:

        t_start = time()

        df = next(df_iter)

        # transform

        # green
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        # load
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest csv data to postgres')
    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--db')
    parser.add_argument('--table_name')
    # parser.add_argument('--url')


    args = parser.parse_args()    

    main(args)