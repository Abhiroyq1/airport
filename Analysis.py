import pandas as pd
import numpy as np
from pandas_profiling import ProfileReport
import dask.dataframe as dd

import matplotlib.pyplot as plt
import seaborn as sns

def analysis_flight(flight_file, airport_dat, list_date_columns_flight):
    flightlist_1 = pd.read_csv(flight_file, parse_dates=list_date_columns_flight)
    # to drop memory overloading, redcing data intake
    flightlist = flightlist_1.iloc[:5000, :]
    print("Information on flight data")
    flightlist_sample = flightlist[['callsign', 'number', 'icao24', 'registration', 'typecode', 'origin',
                                    'destination', 'firstseen', 'lastseen', 'day']].drop_duplicates()
    airport = pd.read_csv(airport_dat, sep=',',header=None)
    airport.columns = ['Airport ID', 'Name', 'City', 'Country', 'IATA', 'ICAO', 'Latitude', 'Longitude', 'Altitude',
                       'TimeZone', 'DST', 'TZ', 'Type', 'Source']
    # Replacing esacpe character in Nan values
    airport['IATA'] = np.where(airport['IATA'] == r'\N', np.NaN, airport['IATA'])
    airport['ICAO'] = np.where(airport['ICAO'] == r'\N', np.NaN, airport['ICAO'])
    # dropping nan values
    airport = airport[airport['IATA'].notna()]
    airport = airport[airport['ICAO'].notna()]
    flightlist_sample = flightlist_sample[flightlist_sample['origin'].notna()]
    # Sampling airport data
    airport_sample = airport[['Name', 'IATA', 'ICAO', 'TZ']].drop_duplicates()
    # merging flight and airport data on the basis of origin and destination airport codes
    flightlist_origin = pd.merge(flightlist_sample, airport_sample, left_on='origin', right_on='ICAO')
    flightlist_destination = pd.merge(flightlist_origin, airport_sample, left_on='destination', right_on='ICAO',
                                      suffixes=['_origin', '_destination'])
    return flightlist_destination.shape
fl_data = r"C:\Users\abhid\Anaconda3\envs\Env2020\Scripts\Jupyternotebooks\Trial_RR\flightlist_20200501_20200531.csv"
airport_d = r"https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
list_date_col = ['firstseen', 'lastseen', 'day']
ana_fl = analysis_flight(fl_data, airport_d, list_date_col)
print(ana_fl)