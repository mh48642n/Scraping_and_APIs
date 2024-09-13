from pandas.tseries.offsets import DateOffset
#from benedict import benedict #developed by fabiocaccamo
from pandas import option_context
from itertools import chain
from datetime import date
import pandas as pd
import requests
import json
import math
import os



        
pd.set_option('display.max_colwidth', None)

#Can work!
class FRED:
    #initiates a class object called key
    def __init__(self, key):
        self.final_part = "&api_key=" + key + "&file_type=json"
        
    #makes the query to grab the variables and pulls the json    
    def json_change(self, name):
        host_query = "https://api.stlouisfed.org/fred/series/observations?"
        series_id = "series_id=" + name

        url = host_query + series_id + self.final_part
        request = requests.get(url)
        request = request.json()

        return(request)

    #collected the series based off the series list
    def collection(self, series):
        first = series.pop(0)
        final_dict = self.pull(first)

        for serie in series: 
            data = pd.DataFrame(self.pull(serie))

            fred_dict = self.wrangling(data, serie)    
            final_dict = pd.merge(final_dict, fred_dict, how = "outer", on = "dates")

        return(final_dict)

    #allows for the collection of exactly one series, required wihen collecting more than one series
    def pull(self, series):
        f_dict = {}
        object = self.json_change(series)
        
        f_dict["dates"] = [object["observations"][y].get("date") for y in range(len(object["observations"]))]
        f_dict[series] = [object["observations"][y].get("value") for y in range(len(object["observations"]))]

        return(self.wrangling(pd.DataFrame(f_dict), series))
   
    #changing the datatype of certain variables
    def wrangling(self, fred_dict, se):
        fred_dict["dates"] = pd.to_datetime(fred_dict["dates"])
        fred_dict[se] = pd.to_numeric(fred_dict[se], errors = "coerce")
        return(fred_dict)

        

#class main:

   #pd.set_option('display.max_colwidth', None)

    #fred = FRED()   
    #fred_data = fred.collection(["DFF","DGS1MO","DGS3MO", "DGS6MO" ,"DGS1", "DGS2", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30"])      
    #print("Fred", fred_data)
    
    #bls = BLS("ee2f076f1d254305bc09f42aa498afab", ['CEU3000000001', 'CUUR0000SA0', 'LNU02000000', 'LNU03000000'])
    #print("BLS")
    #bls_data = bls.collection(1980, 2024, ["man_emp","cpi","employment", "unemp_lvl"])
    #print(type(bls_data))
    
    #bea = BEA()
    #parameters = [{"survey" : "NIPA", "table" : "T20600", "freq" : "M"},
    #              {"survey" : "NIPA", "table" : "T20306", "freq" : "Q"}]
    #bea_data = bea.bea_tables(parameters)
    #print("BEA", bea_data)
    
    
    #data = pd.merge(fred_data, bls_data, how = "left")
    #data = pd.merge(data, bea_data, how = "left")
    
    #print(data)   

    #treasury data
