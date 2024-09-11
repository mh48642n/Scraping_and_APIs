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



class BLS:
    def __init__(self, key, date_1, date_2, rename):
        self.key = key 
        self.date_1 = date_1
        self.date_2 = date_2
        self.rename = rename
        self.header = {'Content-type' : 'application/json'}
    
    def time_series_json(self,first_year,last_year, series):
        data = json.dumps({"seriesid" : series, 
                   "startyear": first_year, "endyear" : last_year, 
                   "registrationkey": self.key})
        res = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data = data, headers = self.header)
        table = self.format_json(json.loads(res.text))
        return(table)
    
    def collection(self, series):
        difference = (self.date_2 - self.date_1)
        if difference > 20:     
            dates = self.split(difference)

            first = self.time_series_json(dates["first_year"][0], dates["second_year"][0], series).reset_index(drop = True, inplace = False)
            
            for x in range(1, 3):
                bls_dict = self.time_series_json(dates["first_year"][x], dates["second_year"][x], series)
                first = pd.merge(first, bls_dict, how = "outer").reset_index(drop = True, inplace = False)

        else:
            bls_dict = self.time_series_json(self.date_1, self.date_2, series)

        for i in range(0, len(series)):
            first = first.rename(columns = {series[i]: self.rename[i]})

        return(first)
    
    def split(self, difference):
        if(difference % 3 != 0):
            split = math.ceil(difference/3)
        else:
            split = difference/3
            
        date1 = int(self.date_1 + split)
        date2 = int(date1 + split)

        print("split = ", split,
                "\ndate_1 = ", self.date_1,
                "\ndate1 = ", date1,
                "\ndate2 = ", date2,
                "\ndate_2 = ", self.date_2  
        )

        dates = {"first_year" : [self.date_1, date1+1, date2+1],"second_year":[date1, date2, self.date_2]}

        return(dates)

    def format_json(self, json):
        bls_dict = {"dates":[],"value":[],"index":[]}   
        
        for i in range(0, len(json['Results']['series'])):
            serie = str(json['Results']['series'][i].get("seriesID"))
            data = json['Results']['series'][i].get("data") 
 
            for items in data:
                bls_dict["dates"].append(items.get("periodName") + " 01 " + items.get("year"))
                bls_dict["value"].append(items.get("value"))         
                bls_dict["index"].append(serie)
                
        bls_dict = self.wrangling(pd.DataFrame(bls_dict))
        return(bls_dict)
    
    def wrangling(self, bls_dict):
        bls_dict["dates"] = pd.to_datetime(bls_dict["dates"])
        bls_dict["value"] = pd.to_numeric(bls_dict["value"], errors = "coerce")
        bls_dict = bls_dict.pivot(index = "dates", columns = "index", values = "value").reset_index(inplace = False)
        
        return(bls_dict)    


