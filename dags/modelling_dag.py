from airflow.decorators import dag, task
from airflow.contrib.hooks.wasb_hook import WasbHook
from helper_functions.data_functions import *
from helper_functions.mutlivarient_functions import *
from helper_functions.univarient_functions import *
from helper_functions.utility import *
from helper_functions.ExponentialSmoothing import *
from datetime import datetime, timedelta
import mlflow
import os


START_DATE = datetime(2023, 2, 1, 0, 0, 0)
END_DATE = datetime(2023, 2, 20, 0, 0, 0)
CONTAINER_NAME = "apar-device-data"

default_args = {"owner": "Kaushal", "retries": 0, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="modelling_dag_v26",
    default_args=default_args,
    start_date=START_DATE,
    end_date=END_DATE,
    schedule_interval="@weekly",
    max_active_runs=1,
    catchup=True,
)
def model():
    @task(multiple_outputs=True, wait_for_downstream=True)
    def startup_task(**kwargs):
        # connecting to airflow wasb
        device_id = "YBEM_B1"
        target_column = "forward_active_energy_difference"
        filter_by_timestamp = "1HR"
        sensors = "D43"
        start_date = "2023-01-01"
        end_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d")
        end_date = datetime.strftime(end_date, "%Y-%m-%d")
        cwd = os.getcwd()
        print(os.listdir(f"{cwd}"))
        # print(os.listdir(f"{cwd}/mlruns"))
        print(os.listdir(f"{cwd}/dags"))
        print(os.listdir(f"{cwd}/dags/helper_functions"))
        return {
            "device_id": device_id,
            "target_column": target_column,
            "filter_by_timestamp": filter_by_timestamp,
            "sensors": sensors,
            "start_date": start_date,
            "end_date": end_date,
        }

    @task(wait_for_downstream=True)
    def multivarient_darts_model(
        device_id,
        target_column,
        filter_by_timestamp,
        sensors,
        start_date,
        end_date,
        **kwargs,
    ):
        model_trained_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d").date()
        series, holiday = create_multivarient_data(
            device_id, start_date, end_date, sensors, filter_by_timestamp
        )

        series, main_scaler = scalered_df(series)
        holiday, scaler = scalered_df(holiday)

        # save the scaler
        dump(main_scaler, open("multivarient_scaler.pkl", "wb"))

        date = datetime.strptime(str(model_trained_date), "%Y-%m-%d").date()
        #
        result_date = model_trained_date - timedelta(days=7)
        date_before_7days = result_date.strftime("%Y-%m-%d")
        date_before_14days = result_date - timedelta(days=7)
        #
        train, val = series.split_before(pd.Timestamp(date_before_7days))
        train_holiday, B = holiday.split_before(pd.Timestamp(date_before_7days))
        A, test_holiday = holiday.split_before(pd.Timestamp(date_before_14days))
        #
        model, forecast = NBEATSModel_multivarient_model_creation(
            train, val, train_holiday, test_holiday
        )
        #
        smapes = smape(val, forecast, n_jobs=-1, verbose=True)
        smape_1 = np.mean(smapes)
        #
        experiment_name = "{}".format(device_id)
        date_1 = date.today()
        run_name = "Multivarient-Darts-N-Beta-{}".format(date_1)
        tag = "MultivarientN-Beta"
        filter_by_timestamp = "1HR"

        darts_create_experiment_multivarient(
            experiment_name, run_name, smape_1, model, tag, date_1, filter_by_timestamp
        )

        print("*************", mlflow.get_tracking_uri())

        experiment = mlflow.get_experiment_by_name(experiment_name)
        print("--", experiment)
        if experiment:
            runs = mlflow.search_runs(experiment_ids=experiment.experiment_id)
            runs_df = pd.DataFrame(runs)
            print("+++", runs_df)
            run_id = runs_df["run_id"][0]

        model_uri = "runs:/{}/nbeats-model".format(run_id)
        from datetime import date

        date = date.today()
        mlflow.register_model(
            model_uri=model_uri,
            name=experiment_name + "_Multivarient",
            tags={"Date": str(date)},
        )

        return 0

    @task(wait_for_downstream=True)
    def exponential_smoothing_darts_model(
        device_id,
        target_column,
        filter_by_timestamp,
        sensors,
        start_date,
        end_date,
        **kwargs,
    ):
        model_trained_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d").date()
        date_1 = datetime.strptime(str(model_trained_date), "%Y-%m-%d").date()

        series = create_exponential_smoothing_data(
            device_id, start_date, end_date, sensors, filter_by_timestamp
        )

        series, main_scaler = scalered_df(series)
        dump(main_scaler, open("ExponentialSmoothing_scaler.pkl", "wb"))

        result_date = date_1 - timedelta(days=7)

        train, val = series.split_before(pd.Timestamp(result_date))

        model, forecast, smapes = ExponentialSmoothing_model_creation(train, val)
        experiment_name = "{}".format(device_id)
        run_name = "Exponential_Smoothing-{}".format(date_1)
        tag = "Exponential_Smoothing"
        #
        darts_create_experiment(
            experiment_name, run_name, smapes, model, date_1, filter_by_timestamp, tag
        )
        #
        #
        experiment = mlflow.get_experiment_by_name(experiment_name)
        print("--", experiment)
        if experiment:
            runs = mlflow.search_runs(experiment_ids=experiment.experiment_id)
            runs_df = pd.DataFrame(runs)
            print("+++", runs_df)
            run_id = runs_df["run_id"][0]

        model_uri = "runs:/{}/nbeats-model".format(run_id)
        mlflow.register_model(
            model_uri=model_uri,
            name=experiment_name + "_Exponential_Smoothing",
            tags={"Date": str(date_1)},
        )

        return 0

    @task(wait_for_downstream=True)
    def univariate_model(
        device_id,
        target_column,
        filter_by_timestamp,
        sensors,
        start_date,
        end_date,
        **kwargs,
    ):
        time_step = 168
        model_trained_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d").date()
        date = datetime.strptime(str(model_trained_date), "%Y-%m-%d").date()

        series = create_univariate_data(
            device_id, start_date, end_date, sensors, filter_by_timestamp
        )
        series_scaled, scaler = scalered_df(series)

        # save the scaler
        dump(scaler, open("scaler.pkl", "wb"))

        train = series_scaled[:-168]
        val = series_scaled[-168:]

        input_chunk_length = 168
        output_chunk_length = 12

        model, forecast = NBEATSModel_model_creation(
            train, time_step, input_chunk_length, output_chunk_length
        )

        smapes = smape(val, forecast, n_jobs=-1, verbose=True)
        smape_1 = np.mean(smapes)

        # Mape=darts_Mape_calculator(val,forecast)
        print("Smape by inbulit function", smape_1)

        #
        experiment_name = "{}".format(device_id)
        print(experiment_name)
        date_1 = date.today()
        run_name = "Univarient-Darts-N-Beta-{}".format(date_1)
        tag = "UnivarientN-Beta"

        # mlflow.set_tracking_uri("http://127.0.0.1:5001")

        print("uri set")

        # mlflow.log_text("Hello", "sda")

        print(mlflow.get_tracking_uri())

        darts_create_experiment(
            experiment_name, run_name, smape_1, model, date_1, filter_by_timestamp, tag
        )

        # (experiment_name, run_name, mape, model, date, filter_by_timestamp)

        # metric_name = "smape"
        experiment = mlflow.get_experiment_by_name(experiment_name)
        print("--", experiment)
        if experiment:
            runs = mlflow.search_runs(experiment_ids=experiment.experiment_id)
            runs_df = pd.DataFrame(runs)
            print("+++", runs_df)
            run_id = runs_df["run_id"][0]

        model_uri = "runs:/{}/nbeats-model".format(run_id)
        mlflow.register_model(model_uri=model_uri, name=experiment_name)

        return 0

    dict_1 = startup_task()
    dict_2 = multivarient_darts_model(
        dict_1["device_id"],
        dict_1["target_column"],
        dict_1["filter_by_timestamp"],
        dict_1["sensors"],
        dict_1["start_date"],
        dict_1["end_date"],
    )
    dict_3 = exponential_smoothing_darts_model(
        dict_1["device_id"],
        dict_1["target_column"],
        dict_1["filter_by_timestamp"],
        dict_1["sensors"],
        dict_1["start_date"],
        dict_1["end_date"],
    )
    dict_4 = univariate_model(
        dict_1["device_id"],
        dict_1["target_column"],
        dict_1["filter_by_timestamp"],
        dict_1["sensors"],
        dict_1["start_date"],
        dict_1["end_date"],
    )


run_dag = model()
run_dag
