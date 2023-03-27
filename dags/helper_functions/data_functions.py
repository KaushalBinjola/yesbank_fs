import pandas as pd
from helper_functions.mutlivarient_functions import *
from helper_functions.utility import *                           #Yes_Bank/UnivarientDarts/univarientNBeta.py
from pickle import dump
from darts.metrics import smape
import azure.storage.blob
from datetime import date
import datetime
from helper_functions.univarient_functions import *

#mlflow.set_tracking_uri("http://127.0.0.1:5000")
import holidays

path="./dags/helper_functions/Holidays_List2020_2024.csv"

def common_data_function(device_id, start_time, end_time, sensors, filter_by_timestamp):
    df=data_import(device_id, start_time,end_time,sensors)
    #df=pd.read_csv(r"C:\Users\Samarth\PycharmProjects\YES_BANK_FORCASTING\data\raw\Fetch.csv")
    df=date_time_features_extraction(df)
    df=filterbytimestamp(df,filter_by_timestamp)
    df=pd.DataFrame(df.to_dict())
    return df

def create_multivarient_data(device_id, start_time, end_time, sensors, filter_by_timestamp):
    df = common_data_function(device_id, start_time, end_time, sensors, filter_by_timestamp)
    df=holiday_creater(path,df)
    df_holiday=df[["time","Indian_Holidays"]]
    df=df[["time","forward_active_energy_difference"]]
    series=create_darts_series(df,"forward_active_energy_difference")
    holiday=create_darts_series(df_holiday,"Indian_Holidays")
    return series, holiday

def create_univariate_data(device_id, start_time, end_time, sensors, filter_by_timestamp):
    df = common_data_function(device_id, start_time, end_time, sensors, filter_by_timestamp)
    df=df[["time","forward_active_energy_difference"]]
    series=univ_create_darts_series(df)
    return series

def create_exponential_smoothing_data(device_id, start_time, end_time, sensors, filter_by_timestamp):
    df = common_data_function(device_id, start_time, end_time, sensors, filter_by_timestamp)
    df=df[["time","forward_active_energy_difference"]]
    series=create_darts_series(df,"forward_active_energy_difference")
    return series
    
    
    
    