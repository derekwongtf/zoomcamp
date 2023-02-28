#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(year: int, month: int, color: str):
    user='root'
    password='root'
    host='localhost'
    port=5432 
    db='docker_prod'
    schema= 'trips_data_all'
    
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    csv_name = f"data/{dataset_file}.csv.gz"
    os.system(f"wget {dataset_url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}',  connect_args={'options': '-csearch_path={}'.format(schema)})

    
    for df in pd.read_csv(csv_name, iterator=True, chunksize=300000):
        t_start = time()
        if color == "yellow":
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
            df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
            table_name='yellow_tripdata'
        elif color == "green":
            df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
            df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
            table_name='green_tripdata'
        else:
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
            df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
            table_name='vfh_tripdata'
        
        df['PULocationID'] = df['PULocationID'].astype("Int64")
        df['DOLocationID'] = df['DOLocationID'].astype("Int64")
    
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('inserted chunk of size %6d, took %.3f second' % (len(df),t_end - t_start))

def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        main(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = range(1,13)
    year = 2019
    etl_parent_flow(months, year, color)
