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
        print("Series: ", series,
              "\nDates: ", first_year, " - ", last_year)
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
        if difference >= 20:     
            dates = self.split(difference, date_1, date_2)
            f_date = dates.pop(0)
            first = self.time_series_json(f_date[0], f_date[1], series).reset_index(drop = True, inplace = False)
            for date in dates:
                print(date[0], date[1])
                bls_dict = self.time_series_json(date[0], date[1], series)
                first = pd.merge(first, bls_dict, how = "outer").reset_index(drop = True, inplace = False)

        else:
            first = self.time_series_json(date_1, date_2, series)


        if(len(rename) != 0):
            if(len(rename) == len(series)):
                for i in range(0, len(series)):
                    first = first.rename(columns = {series[i]: rename[i]})
            else: 
                print("Restart the program with the right amount of column names")

        return first
    
    def split(self, difference,date_1, date_2):
        
        i = 2
        while True:
            value = math.floor(difference/i)
            if (value <= 19.0) and (value >= 9.0):
                break
            else:
                i += 1

        dates = []

        for m in range(0, i):
            if m == 0:
                next = date_1 + value
                dates.append((date_1, next))
            elif m == (i - 1):
                dates.append((next, date_2))
            else:
                next_2 = value + next
                dates.append((next,  next_2))
                next = next_2
            
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
        bls_dict = self.quarter_partition(bls_dict)

        if(confirm == "Annual"):
            print(confirm)
        elif(confirm == "Quarter"):
            print(confirm)
        else:
            print(confirm)
            bls_dict["dates"] = pd.to_datetime(bls_dict["dates"], format = "mixed") 
            bls_dict["dates"] = bls_dict["dates"] + pd.DateOffset(months = 1)

        bls_dict["value"] = pd.to_numeric(bls_dict["value"], errors = "coerce")        
        return bls_dict
 
    def quarter_partition(self, data):
        data["adjust"] = data["dates"].str.contains("Quarter")
        quarters = {"1":"01", "2":"04", "3":"07", "4":"10"}

        for i in range(0 , len(data)):
            if data["adjust"][i] == False:
                pass
            else:
                value = data.loc[i, "dates"]
                print(value)
                data.loc[i, "dates"] = value[-4:] + "-" + quarters[value[0:1]] + "-01"
        
        return(data)

