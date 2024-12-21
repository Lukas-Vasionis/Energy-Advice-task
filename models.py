import datetime
import time
from pprint import pprint

import numpy as np
import pandas as pd
import requests
from zoneinfo import ZoneInfo
from tqdm import tqdm
import utils

import plotly.express as px


class DataMeteo:
    def __init__(self,
                 api_url:str = 'https://api.meteo.lt/v1/',
                 station_code:str = None, #location id for historical data
                 place_code:str = None): #location id for forcast data

        self.api_url = api_url
        self.station_code = utils.validate_location_code(station_code, location_type='station')
        self.place_code = utils.validate_location_code(place_code, location_type='place')
        self.historic_data=None
        self.forecast_data = None

    def get_historic_data(self, date_from: str, date_to: str, path_df_from_csv=None):
        """
        Retrieves historical observation data within defined date range
        First, get_data() gets the list of observations via API
        Second, process_data() structures into pd.DataFrame with datetime index

        :param date_from: lower range of date range(inclusive)
        :param date_to: upper range of date range(inclusive)
        :param path_df_from_csv: can load data from csv for processing
        :return: pd.DataFrame with observation data of defined date range
        """
        def get_data() -> pd.DataFrame:
            """
            Retrieves data of each date within defined date range.
            :return: pd.DataFrame
            """

            # Structuring list of string dates
            dates=pd.date_range(start=date_from,end=date_to)
            dates_strings = [date.strftime("%Y-%m-%d") for date in dates]

            # Structuring list of request urls for each date
            url_root=f"https://api.meteo.lt/v1/stations/{self.station_code}/observations"
            urls=[f"{url_root}/{str(date)}" for date in dates_strings]

            # Calculating time to sleep between requests to follow API constraints
            time_sleep=utils.calculate_sleep_time(requests_to_make=len(urls))

            # Retrieving data in json for each date
            # observations_date_range=[requests.get(url).json()['observations'] for url in urls]
            observations_date_range = []

            for url in tqdm(urls):

                url_data=requests.get(url).json()
                time.sleep(time_sleep)
                try:
                    url_data = url_data['observations']
                except KeyError as KE:
                    error_too_many_requests = url_data=={'error': {'code': 429, 'message': 'Too Many Requests'}}

                    if error_too_many_requests:
                        print("Sleeping 60 sec... too many requests...")
                        time.sleep(60)
                        print("Trying again...")
                        url_data = requests.get(url).json()
                        url_data=url_data['observations']
                    else:
                        pprint(url_data)

                observations_date_range.append(url_data)

            # Flattening list of lists of observations within each date
            observations_date_range_flat= [item for sublist in observations_date_range for item in sublist]


            return pd.DataFrame(observations_date_range_flat)

        def process_data(observations_data: pd.DataFrame) -> pd.DataFrame:
            """

            :param observations_data: df of observations within defined range
            :return: pd.DataFrame() with observation data of defined date range
            """


            # Add station_code:
            observations_data.loc[:, 'station_code']=self.station_code

            # Set observationTimeUtc as pd.DatetimeIndex
            observations_data.loc[:,'observationTimeUtc'] = pd.to_datetime(observations_data['observationTimeUtc'],utc=True)
            observations_data = observations_data.set_index(pd.DatetimeIndex(observations_data['observationTimeUtc']))
            observations_data=observations_data.drop(['observationTimeUtc'], axis=1)

            return observations_data

        # Getting the raw historic data
        if not path_df_from_csv:
            # Retrieve historic data
            self.historic_data=get_data()
        else:
            self.historic_data=pd.read_csv(path_df_from_csv)

        # Process historic data
        self.historic_data=process_data(self.historic_data)
        return self

    def save_historic_data(self, path):
        self.historic_data.to_csv(path)


    def get_forecast_data(self):
        """
        :return: pd.DataFrame, kur indeksas yra laikas (pd.DatetimeIndex) su įvertinta laiko zona;
        """
        def get_data():
            url_root=f"https://api.meteo.lt/v1/places/{self.place_code}/forecasts/long-term" # all places have long-term forecasts only
            data=requests.get(url_root).json()
            return data

        def process_data(data):

            df_forecast=pd.DataFrame(data['forecastTimestamps'])
            # df_forecast.loc[:,'forecastCreationTimeUtc']=data['forecastCreationTimeUtc']
            df_forecast.loc[:,'forecastType']=data['forecastType']

            place_meta=data['place']
            df_forecast.loc[:, 'administrativeDivision'] = place_meta['administrativeDivision']
            df_forecast.loc[:, 'code'] = place_meta['administrativeDivision']
            df_forecast.loc[:, 'latitude'] = place_meta['coordinates']["latitude"]
            df_forecast.loc[:, 'longitude'] = place_meta['coordinates']["longitude"]
            df_forecast.loc[:, 'country'] = place_meta['country']
            df_forecast.loc[:, 'countryCode'] = place_meta['countryCode']
            df_forecast.loc[:, 'name'] = place_meta['name']

            # Set observationTimeUtc as pd.DatetimeIndex
            df_forecast.loc[:, 'forecastTimeUtc'] = pd.to_datetime(df_forecast['forecastTimeUtc'],utc=True)
            df_forecast = df_forecast.set_index(pd.DatetimeIndex(df_forecast['forecastTimeUtc']))
            df_forecast = df_forecast.drop(['forecastTimeUtc'], axis=1)

            # Adding LT time for future analysis
            df_forecast['forecastTime_LT'] = df_forecast.index.tz_convert(
                ZoneInfo("Europe/Vilnius"))  # should take into account daylight saving time as well

            return  df_forecast

        forecast_data = get_data()

        self.forecast_data = process_data(forecast_data)
        return self


class HistAnalysis:
    def __init__(self, df_hist, df_forecast=None):
        self.df_hist = df_hist
        self.df_forecast = df_forecast

        self.temp_mean = None
        self.temp_mean_day = None
        self.temp_mean_night = None
        self.humid_mean = None
        self.n_weekends_w_precip = None

        self.df_hist_n_forecast = None
        self.fig_hist_n_forecast = None

    def processing(self):
        self.df_hist['observationTime_LT'] = self.df_hist.index.tz_convert(
            ZoneInfo("Europe/Vilnius"))  # should take into account daylight saving time as well

        # Adding date for later aggregations
        self.df_hist['Time_LT_date'] = self.df_hist['observationTime_LT'].dt.date
        # self.df_hist['observationTime_LT_time'] = self.df_hist['observationTime_LT'].dt.time

        # Flagging day measurements
        self.df_hist['Time_LT_is_daytime'] = self.df_hist['observationTime_LT'].dt.hour.between(8, 20)

        # Indexing weekend within given period (for weekend forcast data)
        # If df_hist['Time_LT_weekend_rank'] value is none - measurement was done not on weekend
        self.df_hist = self.df_hist.sort_values(by='observationTime_LT', ascending=True)
        self.df_hist['Time_LT_is_weekend'] = self.df_hist['observationTime_LT'].dt.dayofweek>4 # days are indexed in python, therefore monday: 0, thuesday:1, etc...
        self.df_hist['Time_LT_weekend_rank'] = self.df_hist['observationTime_LT'].dt.to_period('W-SUN')  # Group by ISO week (ending Sunday)
        self.df_hist['Time_LT_weekend_rank'] = self.df_hist['Time_LT_weekend_rank'].rank(method='dense')
        self.df_hist['Time_LT_weekend_rank'] = np.where(self.df_hist["Time_LT_is_weekend"], self.df_hist["Time_LT_weekend_rank"], None)


        self.df_hist = self.df_hist.drop(["Time_LT_is_weekend",
                                          # "observationTime_LT",
                                          ], axis=1)
        return self

    def get_mean_metrics(self):
        df_h_temp=self.df_hist
        self.temp_mean = df_h_temp['airTemperature'].mean()
        self.temp_mean_day = df_h_temp.loc[df_h_temp["Time_LT_is_daytime"],'airTemperature'].mean()
        self.temp_mean_night = df_h_temp.loc[~df_h_temp["Time_LT_is_daytime"], 'airTemperature'].mean()

        self.humid_mean = df_h_temp['relativeHumidity'].mean()

        weekend_with_precip=(
            df_h_temp.loc[(df_h_temp["Time_LT_weekend_rank"] is not None) & (df_h_temp["precipitation"] > 0)].agg({"Time_LT_weekend_rank": 'nunique'})
        )
        self.n_weekends_w_precip = weekend_with_precip.iloc[0]

        print('Question 2: Metrics of defined time frame')
        print(f"Mean temp: {self.temp_mean}")
        print(f"Mean humidity: {self.humid_mean}")
        print(f"Mean humidity: {self.humid_mean}\n")

        print(f"Mean temp day: {self.temp_mean_day}")
        print(f"Mean temp night: {self.temp_mean_night}\n")

        print(f"Weekends with precipitation: {self.n_weekends_w_precip}")
        return self

    def compare_hist_n_forecast(self, show_figure=False):
        def get_last_week_data_ISO(df_h):
            df_h['ISO_week'] = df_h['observationTime_LT'].dt.isocalendar().week

            # Get today's ISO week
            iso_week_today = datetime.date.today().isocalendar()
            iso_week_today = iso_week_today[1]

            # Keep the earlier week data
            df_h = df_h.loc[df_h['ISO_week']==iso_week_today-1, :]
            df_h = df_h.drop('ISO_week', axis=1)
            return df_h


        # Process column names prior to concat
        # PROCESS HISTORIC DATA
        df_h_temp = self.df_hist
        df_h_temp = df_h_temp.loc[:, ["observationTime_LT", "airTemperature"]]
        df_h_temp = get_last_week_data_ISO(df_h_temp)
        df_h_temp.loc[:,'type'] = 'historic'
        df_h_temp.rename(columns={"observationTime_LT":"Time_LT"},inplace=True)

        # PROCESS FORECAST DATA
        df_f_temp=self.df_forecast
        df_f_temp = df_f_temp.loc[:,["forecastTime_LT", "airTemperature"]]
        df_f_temp.loc[:, 'type'] = 'forecast'
        df_f_temp.rename(columns={"forecastTime_LT": "Time_LT"}, inplace=True)

        # CONCAT
        df = pd.concat([df_h_temp, df_f_temp])

        self.df_hist_n_forecast=df
        
        fig=px.scatter(df, 'Time_LT', 'airTemperature', color='type', title="3 Užduotis:")

        self.fig_hist_n_forecast=fig

        if show_figure:

            fig.show()

        return self





