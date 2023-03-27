import pandas as pd
# from iosense import *
from iosense_connect import *


def updated_get_data(
    device_id,
    start_time,
    end_time,
    sensors,
):
    """
    Importing data from iosense.io libarary
    device_id:- device Id required to import data
    start_time:- starting time stamp (first data point)
    end_time:- end time stamp (last data point)
    sensors:-sensor ids to import data(features,ex.D1,D2,D0)
    target_col:-dependent column(ex.Foraward active energy)
    filterBy:-frequency of time stamp to select(ex.1MIN,5MIN,15MIN)
    percentage:- percentage of data to split(ex. 0.8,0.7)
    sariam_freqq:- Seasonal factor for Sarimax
    """

    a = DataAccess("634651e605421c3ddb7a39cb", "datads.iosense.io")
    print(
        f"{device_id},start_time={start_time},end_time={end_time},sensors=[{sensors}],cal={False},bands=None,echo=True,"
    )
    df = a.data_query(
        device_id,
        start_time=start_time,
        end_time=end_time,
        sensors=None,
        cal=False,
        bands=None,
        echo=True,
    )

    return df
    