import datetime
import utils
from models import DataMeteo, HistAnalysis



# Setting date_from and date_to
date_today = datetime.date.today().strftime('%Y-%m-%d')
date_year_back = (datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')

# Instance of the object
weather_data = DataMeteo(station_code="vilniaus-ams", place_code='vilnius')

# FORECAST DATA
weather_data = weather_data.get_forecast_data()
# HISTORICAL DATA
weather_data = weather_data.get_historic_data(date_from=date_year_back, date_to=date_today,
                                              path_df_from_csv="outs/weather_data.vilnius.historic.365.csv"
                                              )
# weather_data.save_historic_data("outs/weather_data.vilnius.historic.365.csv")

# DATA ANALYSIS
history_analysis = HistAnalysis(df_hist=weather_data.historic_data,
                                df_forecast=weather_data.forecast_data
                                )
history_analysis = history_analysis.processing()
history_analysis.get_mean_metrics()
history_analysis.compare_hist_n_forecast(show_figure=True)

utils.interpolate_temp(my_series=history_analysis.df_hist_n_forecast['airTemperature'])