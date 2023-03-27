# import pickle
import pandas as pd
from io import StringIO
import os
import pyarrow as pa
import pyarrow.parquet as pq


def check_blob_and_return(conn, container_name, file_name):
    """
    checks if blob exists in container and returns it if exists. Also deletes the blob
    """
    if conn.check_for_blob(
        container_name=container_name,
        blob_name=file_name,
    ):
        cwd = os.getcwd()

        if os.path.exists(f"{cwd}/{file_name}"):
            os.remove(f"{cwd}/{file_name}")

        conn.get_file(f"{file_name}", container_name, file_name)
        df = pd.read_parquet(f"{file_name}")

        os.remove(f"{file_name}")
        return True, df
    return False, []


def create_parquet_and_overwrite(conn, df, container_name, st_date):
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path="./parquet_files/",
        partition_filename_cb=lambda x: f"{st_date.month}-{st_date.year}.parquet",
    )
    print("Created parquet and preparing for loading")
    b = os.path.getsize(f"./parquet_files/{st_date.month}-{st_date.year}.parquet")
    print(b)
    conn.load_file(
        f"./parquet_files/{st_date.month}-{st_date.year}.parquet",
        f"{container_name}",
        f"{st_date.month}-{st_date.year}.parquet",
        overwrite=True,
        length=b,
    )
    print("File loaded to azure")
