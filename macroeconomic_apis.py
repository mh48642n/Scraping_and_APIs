from pandas.tseries.offsets import DateOffset
from benedict import benedict #developed by fabiocaccamo
from pandas import option_context
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

#Can work!
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
            final_dict = pd.merge(final_dict, fred_dict, how = "outer", on = "dates")

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

#Can work
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
            dates = self.split(date_1, date_2, difference)

            first = self.wrangling(dates["first_year"][0], dates["second_year"][0]).reset_index(drop = True, inplace = False)
            
            for x in range(1, 3):
                bls_dict = self.wrangling(dates["first_year"][x], dates["second_year"][x])
                first = pd.merge(first, bls_dict, how = "outer").reset_index(drop = True, inplace = False)

        else:
            bls_dict = self.wrangling(date_1, date_2)

        for i in range(0, len(self.series)):
            first = first.rename(columns = {self.series[i]: rename[i]})

        return(first)
    
    def split(self,date_1,date_2,difference):
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

        return(dates)

    def wrangling(self, date_1, date_2):
        bls_dict = {"dates":[],"value":[],"index":[]}   
        json = self.time_series_json(date_1, date_2)
        
        print(json)
        for i in range(0, len(json['Results']['series'])):
            serie = str(json['Results']['series'][i].get("seriesID"))
            data = json['Results']['series'][i].get("data") 
 
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
    
class BEA:
    def __init__(self, key):
        self.key = key
        self.url = "https://apps.bea.gov/api/data?&UserID=" + self.key

    
    def json_change(self, confirm):
        #going to get surveys and choose survey to see tables
        if(confirm == 1):
            self.pulling_tables("ParamValue")

        #pulls specific data from table within survey 
        elif(confirm == 2):
            stuff = 5


    #getting tables from survey
    def pulling_tables(self):
        self.pullall_survey()
        
        survey = input("Name of survey:")
        m = requests.get(self.url + "&method=GetParameterValues&datasetname=" + survey + "&ParameterName=TableName&ResultFormat=JSON")
        m = m.json()
       
        m = m["BEAAPI"]["Results"]["ParamValue"]
        tables = pd.DataFrame({"Table Name": [m[i].get("TableName") for i in range(0, len(m))],
                               "Description(Intervals)": [m[i].get("Description") for i in range(0, len(m))]})
        
        
        confirm = input("Do you want to view each table(T/F):")
        while confirm == "T":
            value = tables.loc[int(input("Begin:")):int(input("End:")), "Description(Intervals)"]
            print(value)
            confirm = input("Continue(T/F):")
            
    def parameters_survey(self, survey):
        m = requests.get(self.url + "&method=getparameterlist&datasetname=" + survey + "&ResultFormat=JSON")
        m = m.json()

        m = m["BEAAPI"]["Results"].get("Parameter")
        parameter = pd.DataFrame({"Parameter": [p.get("ParameterName") for p in m],
                                  "DataType": [p.get("ParameterDataType") for p in m],
                                  "Description": [p.get("ParameterDescription") for p in m],
                                  "Required(1 = True)": [p.get("ParameterIsRequiredFlag") for p in m],
                                  "All value": [p.get("AllValue") for p in m]})
        
        #print(parameter)

        list = parameter.loc[:,"Parameter"].tolist()

        self.parameter_vals(survey, list)
        

            
    def parameter_vals(self, survey, parameters):
        for parameter in parameters:   
            re = requests.get(self.url + "&method=GetParameterValues&datasetname=" + survey + "&ParameterName=" + parameter + "&ResultFormat=JSON")
            re = re.json()

            parameter_values = re["BEAAPI"]["Results"].get("ParamValue")
            keys = list(parameter_values[0].keys())

            dict = {}
            number = 0
            for key in keys:
                print("Here: ", number)
                dict[key] = [k.get(key) for k in parameter_values]
                number = number + 1

            print(pd.DataFrame(dict))

            

        
    def pullall_survey(self):
        m = requests.get(self.url + "&method=GETDATASETLIST&ResultFormat=JSON")
        m = m.json()

        m = m["BEAAPI"]["Results"]["Dataset"]
        surveys = pd.DataFrame({"Dataset": [m[i].get("DatasetName") for i in range(0, len(m))], 
                                "Dataset Description": [m[i].get("DatasetDescription") for i in range(0, len(m))]})
        #self.format_table(surveys)
        print(surveys)

    def pulldata_table(self, ):
        url = self.url + "&method=GetData&DataSetName=" + input("Name of survey:") + "&TableName=" + input("Name of table:") + "&tableID=ALL&Frequency=" + input("Freq of data:") +"&Year=ALL&ResultFormat=JSON"      

        re = requests.get(url)
        re = re.json()
        data = re["BEAAPI"]["Results"].get("Data")

        keys = list(data[0].keys())
        names = []

        #checking what attributes did you need from each dictionary
        if(input("Does user need all attributes for each observation") == "T"):
            print(keys)
            for key in keys

        #changing names of dataframe columns
        if(input("Does user want to change all names of data points(T/F): ") == "T"):
            for index in range(0, len(keys)):
                names = names.insert(index, input("Insert name you want for[{keys[index]}]:"))
        else:
            names = keys

        #pull data from json file that you need
        dict = {}
        for index in range(0, len(keys)):
            dict[names[index]] = [d.get(keys[index]) for d in data]
        re = pd.DataFrame(dict)

        


        print(re)

        
class main:
    pd.set_option('display.max_colwidth', None)

    #fred_data = FRED("565e55b6fbd720965afae15454629fae")   
    #data = fred_data.collection("DFF", ["DGS2", "DGS1"])  
    #print(data)  

    #bls_data = BLS("ee2f076f1d254305bc09f42aa498afab", ['CES3000000001', 'CUSR0000SA0', 'LNS12000000', 'LNS13000000'])
    #data = bls_data.collection(1980, 2022, ["man_emp","core_cpi","employment", "unemp_lvl"])
    #print(data)

    bea_data = BEA("A2D2AB50-A251-4378-8CC1-95E51C78615E")
    bea_data.pulldata_table()
    
                
                
        
        
            
        

 