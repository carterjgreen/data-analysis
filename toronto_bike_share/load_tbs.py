import io
import logging
import os
from collections.abc import Iterable
from pathlib import Path
from zipfile import ZipFile

import duckdb
import requests
from duckdb import DuckDBPyConnection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger()

BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/7e876c24-177c-4605-9cef-e50dd74c617f/resource"
YEAR_MAP = {
    2021: "ddc039f6-07fa-47a3-a707-0121ade3b307",
    2022: "db10a7b1-2702-481c-b7f0-0c67070104bb",
    2023: "f0fa6a67-4571-4dd6-9d5a-df010ebed7d1",
    2024: "9a9a0163-8114-447c-bf66-790b1a92da51",
}
REMOVE_FOLDER = {2022, 2023}
BAD_YEAR = 2022


def setup_duckdb(conn: DuckDBPyConnection) -> None:
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    conn.execute("SET threads TO 2;")


# TODO: Simplify with fsspec `zip://file 202*.csv`
def download_data(years: Iterable[int], unzip: bool = True) -> None:
    LOGGER.info("Beginning data download")
    for year, resource_id in filter(lambda x: x[0] in years, YEAR_MAP.items()):
        LOGGER.info("Downloading data for year: %s", year)
        url = f"{BASE_URL}/{resource_id}/download/bikeshare-ridership-{year}.zip"
        r = requests.get(url, timeout=60)
        z = ZipFile(io.BytesIO(r.content))
        z.extractall("data")

        if not unzip:
            continue

        # Special case
        if year == BAD_YEAR:
            jank_file = "data/bikeshare-ridership-2022/Bike share ridership 2022-11.zip"
            with ZipFile(jank_file) as z:
                z.extractall("data/bikeshare-ridership-2022")
            Path(jank_file).unlink()

        if year in REMOVE_FOLDER:
            for root, _, files in os.walk(f"data/bikeshare-ridership-{year}"):
                for file in files:
                    Path(root, file).rename(Path("data", file))
                Path(root).rmdir()


def remove_csv() -> None:
    LOGGER.info("Removing csv files")
    for root, _, files in os.walk("data"):
        for file in files:
            path = Path(root, file)
            if path.is_file() and path.suffix == ".csv":
                path.unlink()


def load_data(conn: DuckDBPyConnection) -> None:
    try:
        conn.sql("""CREATE TYPE user AS ENUM ('Casual Member', 'Annual Member')""")
        conn.sql("""CREATE TYPE bike AS ENUM ('EFIT G5', 'ICONIC', 'EFIT')""")
    except duckdb.CatalogException:
        LOGGER.info("`User Type` enum is already defined")

    LOGGER.debug("Creating table tbs_trips")
    conn.sql("""
    CREATE TABLE IF NOT EXISTS tbs_trips (
        "Trip Id" INTEGER,
        "Trip  Duration" INTEGER,
        "Start Station Id" INTEGER,
        "Start Time" TIMESTAMP,
        "Start Station Name" VARCHAR,
        "End Station Id" INTEGER,
        "End Time" TIMESTAMP,
        "End Station Name" VARCHAR,
        "Bike Id" INTEGER,
        "User Type" user,
        "Model" bike);
             """)

    LOGGER.info("Inserting new data")

    num_rows = conn.execute("""
    INSERT INTO tbs_trips
    SELECT
        "Trip Id",
        "Trip  Duration",
        "Start Station Id",
        "Start Time",
        "Start Station Name",
        "End Station Id",
        "End Time",
        "End Station Name",
        "Bike Id",
        "User Type",
        "Model"
    FROM read_csv(
        'data/Bike share ridership 202*.csv',
        ignore_errors=true,
        timestampformat='%m/%d/%Y %H:%M',
        allow_quoted_nulls=false,
        union_by_name=true,
        nullstr='NULL'
        ) AS new_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM tbs_trips
        WHERE tbs_trips."Trip Id" = new_data."Trip Id"
    );
    """).fetchone()

    if num_rows:
        LOGGER.info("Inserted %s rows", num_rows[0])


def export_as_parquet(conn: DuckDBPyConnection) -> None:
    LOGGER.info("Saving to parquet")
    conn.execute(
        """
        COPY
            (SELECT * FROM tbs_trips ORDER BY "Trip Id")
        TO "./data/tbs.parquet"
        (FORMAT 'parquet', CODEC 'zstd', COMPRESSION_LEVEL 3);
        """
    )


if __name__ == "__main__":
    conn = duckdb.connect("data/tbs.db")
    setup_duckdb(conn)
    download_data(YEAR_MAP.keys())
    load_data(conn)
    remove_csv()
    export_as_parquet(conn)
    LOGGER.info("Data load complete")
