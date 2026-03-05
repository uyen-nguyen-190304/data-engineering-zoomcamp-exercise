#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def ingest_csv(engine, url, table_name, chunksize=100000, dtype=None, parse_dates=None):
    """
    Stream a CSV (local path or URL) into Postgres using pandas chunks
    Create/overwrite the table on the first chunk, then append
    """
    df_iter = pd.read_csv(
        url,
        iterator=True,
        chunksize=chunksize,
        dtype=dtype,
        parse_dates=parse_dates
    )

    first=True
    for df_chunk in tqdm(df_iter, desc=f"Ingesting -> {table_name}"):
        if first:
            # Create table schema
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False
        
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option("--zones-table", default="zones", help="Zones lookup table")
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
@click.option("--load-zones/--no-load-zones", default=True, help="Whether to load zones table")


def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, zones_table, chunksize, load_zones):
    """
    Ingest NYC taxi trip data + zones lookup into PostgreSQL database
    """
    taxi_url = (
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
        f"yellow_tripdata_{year}-{month:02d}.csv.gz"
    )

    zones_url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/"
        "taxi_zone_lookup.csv"
    )

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    ingest_csv(
        engine=engine,
        url=taxi_url,
        table_name=target_table,
        chunksize=chunksize,
        dtype=dtype,
        parse_dates=parse_dates
    )

    if load_zones:
        ingest_csv(
            engine=engine,
            url=zones_url,
            table_name=zones_table,
            chunksize=chunksize
        )


if __name__ == '__main__':
    run()
    