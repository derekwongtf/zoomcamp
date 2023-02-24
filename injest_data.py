#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    schema = params.schema
    table_name = params.table_name
    url = params.url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}',  connect_args={'options': '-csearch_path={}'.format(schema)})

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df['PULocationID'] = df['PULocationID'].astype("Int64")
    df['DOLocationID'] = df['DOLocationID'].astype("Int64")
    df['payment_type'] = df['payment_type'].astype("Int64")

    # df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df['PULocationID'] = df['PULocationID'].astype("Int64")
            df['DOLocationID'] = df['DOLocationID'].astype("Int64")
            df['payment_type'] = df['payment_type'].astype("Int64")
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', nargs='?',const=1,default='root' ,help='user name for postgres')
    parser.add_argument('--password', nargs='?',const=1,default='root', help='password for postgres')
    parser.add_argument('--host', nargs='?',const=1,default='localhost', help='host for postgres')
    parser.add_argument('--port', nargs='?',const=1,default='5432', help='port for postgres')
    parser.add_argument('--db', nargs='?',const=1,default='docker_prod',help='database name for postgres')
    parser.add_argument('--schema', nargs='?',const=1,default='trips_data_all' ,help='schema name for postgre')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)

    #  python3 injest_data.py --user root --password root --host localhost --port 5432 --db docker_prod --schema trips_data_all --table_name green_tripdata --url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
