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
from pandas._libs.tslibs.parsing import DateParseError
import datetime
from datetime import datetime


class BLS:
    def __init__(self, key):
        self.key = key
        self.header = {'Content-type' : 'application/json'}
        
    def time_series_json(self,first_year,last_year, series):
        data = json.dumps({"seriesid" : series, 
                   "startyear": first_year, "endyear" : last_year, 
                   "registrationkey": self.key})
        res = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data = data, headers = self.header)
        table = self.format_json(json.loads(res.text))
        return table
    
    def collection(self, bundle):
        
        series = bundle[0] 
        date_1 = bundle[1]
        date_2 = bundle[2]
        rename = bundle[3]

        difference = (date_2 - date_1)
        if difference > 20:     
            dates = self.split(difference, date_1, date_2)
            first = self.time_series_json(dates["first_year"][0], dates["second_year"][0], series).reset_index(drop = True, inplace = False)
            
            for x in range(1, 3):
                bls_dict = self.time_series_json(dates["first_year"][x], dates["second_year"][x], series)
                first = pd.merge(first, bls_dict, how = "outer").reset_index(drop = True, inplace = False)

        else:
            bls_dict = self.time_series_json(date_1, date_2, series)

        if(len(rename) == 0):
            pass
        else:
            if(len(rename) == len(series)):
                for i in range(0, len(series)):
                    first = first.rename(columns = {series[i]: rename[i]})
            else: 
                print("Restart the program with the right amount of column names")

        return first
    
    def split(self, difference,date_1, date_2):
        if(difference % 3 != 0):
            split = math.ceil(difference/3)
        else:
            split = difference/3
            
        date1 = int(date_1 + split)
        date2 = int(date1 + split)

        # print("split = ", split,
        #       "\ndate_1 = ", date_1,
        #       "\ndate1 = ", date1,
        #       "\ndate2 = ", date2,
        #       "\ndate_2 = ", date_2  
        # )

        dates = {"first_year" : [date_1, date1+1, date2+1],"second_year":[date1, date2, date_2]}

        return dates

    def format_json(self, json):
        bls_dict = {"dates":[],"value":[],"series":[]}   
        
        for i in range(0, len(json['Results']['series'])):
            serie = str(json['Results']['series'][i].get("seriesID"))
            data = json['Results']['series'][i].get("data") 

            for items in data:
                
                bls_dict["value"].append(items.get("value"))         
                bls_dict["series"].append(serie)


                if(items.get("periodName") == "Annual"):
                    confirm = "Annual"
                    bls_dict["dates"].append(items.get("year"))
                elif(items.get("periodName") == "Quarter"):
                    confirm = "Quarter"
                else:
                    confirm = "Monthly"
                    bls_dict["dates"].append(items.get("periodName") + " 01 " + items.get("year"))
        
                
        bls_dict = self.wrangling(pd.DataFrame(bls_dict), confirm)
        bls_dict = self.pivoting(bls_dict)
        return bls_dict
    
    def pivoting(self, bls_dict):

        description = bls_dict["series"].unique()
        concat = pd.DataFrame()        

        for item in description:
            df = bls_dict[bls_dict["series"] == item]
            df = df.pivot_table(index = "dates", columns = "series", values = "value", aggfunc = 'first')
            concat = pd.concat([concat, df], axis = 1)

        concat = concat.reset_index(drop = False)
        return concat


    def wrangling(self, bls_dict, confirm):
    
        if(confirm == "Annual"):
            print(confirm)
        elif(confirm == "Quarter"):
            print(confirm)
        else:
            print(confirm)
            bls_dict["dates"] = pd.to_datetime(bls_dict["dates"]) 
            bls_dict["dates"] = bls_dict["dates"] + pd.DateOffset(months = 1)

        
        bls_dict["value"] = pd.to_numeric(bls_dict["value"], errors = "coerce")
        #bls_dict = bls_dict.pivot(index = "dates", columns = "index", values = "value").reset_index(inplace = False)

        
        return bls_dict
 

