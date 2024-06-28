from pandas.tseries.offsets import DateOffset
from datetime import date
import pandas as pd
import requests
import json
import math
import os


#Need dataset of these variables
# Macroeconomic variables: Federal Funds Rate, Labor Participation Force Rate, Economic Growth 
# Economic Release Dates: Unemployment, CPI(core and all), Economic growth, PMI, 
# Companies: 15 years of small cap companies 
# Idiosyncractic risks for small cap companies: SEC api  
# Treasury Data: Yields for different bond types, quantity of each across time, how much does the Fed have
# Public Debt: Ceilings, Suspension, Policy releasing
 
class FRED:
    #initiates a class object called key
    def __init__(self, key):
        self.key = key
        
    #makes the query to grab the variables and pulls the json    
    def json_change(self, name):
        host_query = "https://api.stlouisfed.org/fred/series/observations?"
        series_id = "series_id=" + name
        api_key= "&api_key="+ self.key + "&file_type=json"

        url = host_query + series_id + api_key
        request = requests.get(url)
        request = request.json()

        return(request)

    #collected the series based off the series list
    def collection(self, first, series):
        final_dict = self.first(first)

        for se in series: 
            fred_dict = {}
            object = self.json_change(se)  

            fred_dict["dates"] = [object["observations"][y].get("date") for y in range(len(object["observations"]))]
            fred_dict[se] = [object["observations"][y].get("value") for y in range(len(object["observations"]))]

            fred_dict = self.wrangling(pd.DataFrame(fred_dict), se)    
            final_dict = pd.merge(final_dict, fred_dict, how = "left")

        return(final_dict)

    #allows for the collection of exactly one series, required wihen collecting more than one series
    def first(self, first):
        first_dict = {}
        series = first
    
        object = self.json_change(series)
        
        first_dict["dates"] = [object["observations"][y].get("date") for y in range(len(object["observations"]))]
        first_dict[series] = [object["observations"][y].get("value") for y in range(len(object["observations"]))]

        return(self.wrangling(pd.DataFrame(first_dict), series))
   
    #changing the datatype of certain variables
    def wrangling(self, fred_dict, se):
        fred_dict["dates"] = pd.to_datetime(fred_dict["dates"])
        fred_dict[se] = pd.to_numeric(fred_dict[se], errors = "coerce")
        return(fred_dict)

class BLS:
    def __init__(self, key, series):
        self.key = key 
        self.series = series
        self.header = {'Content-type' : 'application/json'}
    
    def time_series_json(self,first_year,last_year):
        data = json.dumps({"seriesid" : self.series, 
                   "startyear": first_year, "endyear" : last_year, 
                   "registrationkey": self.key})
        res = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data = data, headers = self.header)
        return(json.loads(res.text))
    
    def collection(self, date_1, date_2, rename):
        difference = (date_2 - date_1)
        if difference > 20:     
            if(difference % 3 != 0):
                split = math.ceil(difference/3)
            else:
                split = difference/3
                
            date1 = int(date_1 + split)
            date2 = int(date1 + split)

            
            print("split = ", split,
                  "\ndate_1 = ", date_1,
                  "\ndate1 = ", date1,
                  "\ndate2 = ", date2,
                  "\ndate_2 = ", date_2  
            )

            dates = {"first_year" : [date_1, date1+1, date2+1],"second_year":[date1, date2, date_2]}
  
            first = self.wrangling(dates["first_year"][0], dates["second_year"][0]).reset_index(drop = True, inplace = False)
            #print(first)
            
            for x in range(1, 3):
                bls_dict = self.wrangling(dates["first_year"][x], dates["second_year"][x])
                first = pd.merge(first, bls_dict, how = "outer").reset_index(drop = True, inplace = False)

        else:
            bls_dict = self.wrangling(date_1, date_2)

        for i in range(0, len(self.series)):
            first = first.rename(columns = {self.series[i]: rename[i]})


        return(first)
    
    def split(self,difference):
               
        return(split)

    def wrangling(self, date_1, date_2):
        bls_dict = {"dates":[],"value":[],"index":[]}   
        json = self.time_series_json(date_1, date_2)
        
        print(json)
        for i in range(0, len(json['Results']['series'])):
            serie = str(json['Results']['series'][i].get("seriesID"))
            data = json['Results']['series'][i].get("data") 

            bls_dict["dates"] 
            for items in data:
                bls_dict["dates"].append(items.get("periodName") + " 01 " + items.get("year"))
                bls_dict["value"].append(items.get("value"))         
                bls_dict["index"].append(serie)
        bls_dict = pd.DataFrame(bls_dict)
        bls_dict = self.clean(bls_dict)
        return(bls_dict)
    
    def clean(self, bls_dict):
        bls_dict["dates"] = pd.to_datetime(bls_dict["dates"])
        bls_dict["value"] = pd.to_numeric(bls_dict["value"], errors = "coerce")
        bls_dict = bls_dict.pivot(index = "dates", columns = "index", values = "value").reset_index(inplace = False)
        print(bls_dict)
        
        return(bls_dict)
#class BEA:



class main:
    #fred_data = FRED("565e55b6fbd720965afae15454629fae")   
    #data = fred_data.collection("DFF", ["DGS2", "DGS1"])  
    #print(data)  

    bls_data = BLS("ee2f076f1d254305bc09f42aa498afab", ['CES3000000001', 'CUSR0000SA0', 'LNS12000000', 'LNS13000000'])
    data = bls_data.collection(1980, 2022, ["man_emp","core_cpi","employment", "unemp_lvl"])
    print(data)

                
                
                
        
        
            
        

 