#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import click


dtype = {
    "VendorID":                  "Int64",
    "passenger_count":           "Int64",
    "trip_distance":             "float64",
    "RatecodeID":                "Int64",
    "store_and_fwd_flag":        "string",
    "PULocationID":              "Int64",
    "DOLocationID":              "Int64",
    "payment_type":              "Int64",
    "fare_amount":               "float64",
    "extra":                     "float64",
    "mta_tax":                   "float64",
    "tip_amount":                "float64",
    "tolls_amount":              "float64",
    "improvement_surcharge":     "float64",
    "total_amount":              "float64",
    "congestion_surcharge":      "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
def ingest_data(user, password, host, port, db, table):
    """
    Ingest hardcoded NYC Yellow Taxi data (Jan 2021) into PostgreSQL in chunks.
    """
    year = 2021
    month = 1
    chunksize = 100000

    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz'

    # Connect to PostgreSQL
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # Read CSV in chunks
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first_chunk = True
    for df_chunk in tqdm(df_iter):
        if first_chunk:
            df_chunk.head(0).to_sql(
                name=table,
                con=engine,
                if_exists='replace',
                index=False
            )
            first_chunk = False

        df_chunk.to_sql(
            name=table,
            con=engine,
            if_exists='append',
            index=False
        )

    print(f"Finished loading {table} for {year}-{month:02d}")


if __name__ == "__main__":
    ingest_data()
