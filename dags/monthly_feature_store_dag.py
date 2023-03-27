from airflow.decorators import dag, task
from airflow.contrib.hooks.wasb_hook import WasbHook
from helper_functions.azure_functions import *
from helper_functions.data_functions_ioconnect import *
from datetime import datetime, timedelta

START_DATE = datetime(2022, 3, 25, 0, 0, 0)
# END_DATE = datetime(2023, 2, 1, 0, 0, 0)
CONTAINER_NAME = "apar-device-data"

default_args = {"owner": "Kaushal", "retries": 5, "retry_delay": timedelta(seconds=20)}


@dag(
    dag_id="monthly_parquet_feature_store_v10",
    default_args=default_args,
    start_date=START_DATE,
    # end_date=END_DATE,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=True,
)
def data_transform():
    @task(wait_for_downstream=True)
    def data_import(device_id, sensors, **kwargs):
        # connecting to airflow wasb
        blob_conn = WasbHook(wasb_conn_id="azure_blob_connection")
        exec_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d")
        start_time = datetime.strftime(exec_date - timedelta(days=1), "%Y-%m-%d")
        end_time = datetime.strftime(exec_date, "%Y-%m-%d")

        print(start_time, end_time)

        df = updated_get_data(
            device_id,
            start_time,
            end_time,
            sensors,
        )

        print("All data fetched!")
        st_date = datetime.strptime(start_time, "%Y-%m-%d")

        if df is None:
            return 0

        print(df)
        # get months data from azure so that we can append
        flag, return_data = check_blob_and_return(
            blob_conn,
            "apar-device-data/YBEM_B1",
            f"{st_date.month}-{st_date.year}.parquet",
        )

        if flag != False:
            df = pd.concat([df, return_data[1:]], axis=0, ignore_index=True)
        else:
            if exec_date - START_DATE != timedelta(days=0):
                df = df[1:]

        # create parquet and upload to azure
        create_parquet_and_overwrite(blob_conn, df, "apar-device-data/YBEM_B1", st_date)

        return 0

    dict_1 = data_import(
        device_id="YBEM_B1",
        sensors="D5",
    )


run_dag = data_transform()
run_dag
