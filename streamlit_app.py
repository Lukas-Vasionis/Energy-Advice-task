import streamlit as st
import datetime
import models

st.set_page_config(layout="wide")

st.title("Energy Advice užduotys analitiko pozicijai")

st.markdown(
"""
    ### 1 Užduotis:
    
    Sukurta klasė duomenų nuskaitymui ją rasite models.DataMeteo
    
    Klasėje reikia pateikti prognozinių ir/arba istorinių duomenų vietoves identificatorius
     - station_code - istoriniams duomenims
     - place_code - prognoziniams duomenims
    
    
    Klasei sukurti pražomi metodai:
    - **get_historic_data(self, date_from, date_to, path_df_from_csv=None)** - detalesnį aprašymą rasite kode
      - processas suskaidytas į dvi dalis - duomenų nusiėmimą ir duomenų apdorojimą 
        - *Duomenų nuskaitymas* vyksta iteruojant per datas tarp date_from ir date_to. 
            - Nusiimt metinius duomenis užtrunka, 
        nes yra API apribojimas: 180 užklausų per minutę. Del to pridėjau papildomą algoritmą kad sumažinti užklausų dažnį. 
            -   Dėl šio apribojimo prailgsta duomenų nusiėmimas. 
                -   Dėlto pridėjau metoda nusiimtų duomenų išsaugojimaui `DataMeteo.save_historic_data()` 
                -   O į `DataMeteo.get_historic_data()` pridėjau argumentą `path_df_from_csv` duomenų nuskaitymui iš failo o ne iš API
        - *Duomenų apdorojimo*  žingsnis tik prideda užduotyje reikalujamą index'ą
      - Istoriniai duomenys patalpinami į self.historic_data atributą
    - **get_forecast_data()** procesas suskaidytas į tas pačias dalis: duomenų gavimas ir apdorojimas
        - Kadangi duomenys maži, jiems išsaugojimo/užkrovimo mechanizmų nesukūriau 
    
    ---
    
    Duomenų nusiėmimo pavyzdys:
"""
)


st.code("""
        # Instance of the object
        weather_data=DataMeteo(station_code="vilniaus-ams", place_code='vilnius')
    
        # FORECAST DATA
        weather_data = weather_data.get_forecast_data()
        # HISTORICAL DATA
        weather_data = weather_data.get_historic_data(date_from=date_year_back, date_to=date_today,
                                                      path_df_from_csv="outs/weather_data.vilnius.historic.365.csv"
                                                      )
        # weather_data.save_historic_data("outs/weather_data.vilnius.historic.365.csv")
""")

# Setting date_from and date_to
date_today = datetime.date.today().strftime('%Y-%m-%d')
date_year_back = (datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')

# Instance of the object
weather_data = models.DataMeteo(station_code="vilniaus-ams", place_code='vilnius')

# FORECAST DATA
weather_data = weather_data.get_forecast_data()
# HISTORICAL DATA
weather_data = weather_data.get_historic_data(date_from=date_year_back, date_to=date_today,
                                              path_df_from_csv="outs/weather_data.vilnius.historic.365.csv"
                                              )

st.markdown("""#### Preview of Historical data """)
st.code("""weather_data.historic_data""")
st.table(weather_data.historic_data.head())

st.markdown("""#### Preview of Forecast data """)
st.code("""weather_data.forecast_data""")
st.table(weather_data.forecast_data.head())

st.markdown(
    """
    ---
    # 2 Užduotis
    
    Duomenų analizei sukuriau naują klasę `HistAnalysis` į kurios atributus sudedami analizės rezultatai.
    
    Būtina nurodyti istorinių ir prognozinių duomenų pd.DataFrame
    """
)

st.code(
    """# DATA ANALYSIS
    history_analysis = HistAnalysis(df_hist=weather_data.historic_data,
                                    df_forecast=weather_data.forecast_data
                                    )
    history_analysis = history_analysis.processing()
    """)


# DATA ANALYSIS
history_analysis = models.HistAnalysis(df_hist=weather_data.historic_data,
                                df_forecast=weather_data.forecast_data
                                )
history_analysis = history_analysis.processing()
history_analysis.get_mean_metrics()


f"""
### Prašomi rezultatai
- Vidutinė metų temperatūra {round(history_analysis.temp_mean,2)} 
- Vidutinė metų oro drėgmė {round(history_analysis.humid_mean,2)};
- Vidutinė dieninė temperatūra {round(history_analysis.temp_mean_day,2)}
- Vidutitnė naktinė temperatūra {round(history_analysis.temp_mean_night,2)}
- Savaitgalių su krituliais kiekis {history_analysis.n_weekends_w_precip}

"""

f"""
### 3 Užduotis: istoriniai praeitos savaitės duomenys ir ateinančios prognozės

Užduočiai atlikti sukūriau metodą HistAnalysis.compare_hist_n_forecast. Jei leidžiama ne per streamlit aplikaciją, 
nurodykit argumentą `show_figure=True` kad pamatyti grafiką 
"""

st.code(
    """history_analysis=history_analysis.compare_hist_n_forecast(show_figure=False)"""
)


history_analysis=history_analysis.compare_hist_n_forecast(show_figure=False)

st.plotly_chart(history_analysis.fig_hist_n_forecast)

"""
### 4 užduotis

Prašoma funkcija randasi `utils.py`  bet pridedu ją ir čia:
"""

st.code(
    """
    def interpolate_temp(my_series):

        # Number of None values (new values) to insert
        n = 12 # there are twelve 5 minute intervals in one hour
    
        # Creating Series with None values interleaved
        series_interpolated = pd.Series(np.concatenate([[v] + [None] * n for v in my_series]))
    
        series_interpolated=series_interpolated.fillna(np.nan).interpolate()
        print(series_interpolated.to_string())
    """
)
