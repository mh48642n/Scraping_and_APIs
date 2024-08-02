from pandas.tseries.offsets import DateOffset
#from benedict import benedict #developed by fabiocaccamo
from abc import ABC
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
# Treasury Data: Yields for different bond types, quantity of each across time, how much does the Fed have(New York Fed)
# Public Debt: Ceilings, Suspension, Policy releasing
        


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

            first = self.format_json(dates["first_year"][0], dates["second_year"][0]).reset_index(drop = True, inplace = False)
            
            for x in range(1, 3):
                bls_dict = self.format_json(dates["first_year"][x], dates["second_year"][x])
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

    def format_json(self, date_1, date_2):
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
        bls_dict = self.wrangling(bls_dict)
        return(bls_dict)
    
    def wrangling(self, bls_dict):
        bls_dict["dates"] = pd.to_datetime(bls_dict["dates"])
        bls_dict["value"] = pd.to_numeric(bls_dict["value"], errors = "coerce")
        bls_dict = bls_dict.pivot(index = "dates", columns = "index", values = "value").reset_index(inplace = False)
        print(bls_dict)
        
        return(bls_dict)    

class BEA:
    def __init__(self, key):
        self.url = "https://apps.bea.gov/api/data?&UserID=" + key

    #gets all surveys
    def pullall_survey(self):
        url = self.url + "&method=GETDATASETLIST&ResultFormat=JSON"
        surveys = self.format_json(url, "Dataset")
        print(surveys)

    #getting tables from survey
    def pulling_tables(self):
        self.pullall_survey()
        
        confirm = input("Do you want to see some tables in a specific survey(T/F): ")
        if confirm == "T":    
            url = self.url + "&method=GetParameterValues&datasetname=" + input("Name of survey:") + "&ParameterName=TableName&ResultFormat=JSON"
            tables  = self.format_json(url, "ParamValue")
            print("\n\nNumber of tables in the survey", len(tables))
            print(tables)

            confirm = input("Do you want to view each table(T/F):")
            while confirm == "T":
                value = tables.loc[int(input("Begin:")):int(input("End:")), ]
                print(value)
                confirm = input("Continue(T/F):")

    #get parameters from survey       
    def parameters_survey(self):
        survey = input("Input survey: ")
        url = self.url + "&method=getparameterlist&datasetname=" + survey + "&ResultFormat=JSON"
        parameter = self.format_json(url, "Parameter")
        list = parameter.loc[:,"ParameterName"].tolist()
        self.parameter_vals(survey, list)
        
    #get values for each parameter  
    def parameter_vals(self, survey, parameters):
        for parameter in parameters:   
            url = self.url + "&method=GetParameterValues&datasetname=" + survey + "&ParameterName=" + parameter + "&ResultFormat=JSON"
            surveys = self.format_json(url, "ParamValue")
            print(surveys)

            #confirm = input("Do you want to scroll through each table(T/F):")
            #while confirm == "T" :
            #    value = surveys.loc[int(input("Begin:")):int(input("End:")), :2]
            #    print(value)
            #    confirm = input("Continue(T/F): ")


    #get data table from survey
    def pulldata_table(self, survey, table, frequency):
        #survey = input("Name of survey: ")
        #table = input("Table Name: ")

        #checking whether user needs to see the frequencies of table
        #check = input("Would you want to check the frequency of the table(Yes/No): ")
        #if check == "Yes":
        #    self.check_freq(survey, table)

        #frequency = input("Freq of Data: ")
        url = self.url + "&method=GetData&DataSetName=" + survey + "&TableName=" + table + "&tableID=ALL&Frequency=" + frequency +"&Year=ALL&ResultFormat=JSON"  
        returned = self.format_json(url, "Data")

        if(frequency == "M"):
            returned["TimePeriod"] = returned["TimePeriod"].str[:4] + "-" + returned["TimePeriod"].str[5:] + "-" + "01"
            returned["dates"] = pd.to_datetime(returned["TimePeriod"])
        elif(frequency == "Q"):
            returned["TimePeriod"] = returned["TimePeriod"].str[:4] + "-" + returned["TimePeriod"].str[4:] 
            returned["dates"] = pd.PeriodIndex(returned["TimePeriod"], freq = 'Q').to_timestamp() + pd.DateOffset(months = 3)
        elif(frequency == "A"):
            returned["TimePeriod"] = returned["TimePeriod"] + "-12-31"  
            returned["dates"] = pd.to_datetime(returned["TimePeriod"]) 


        returned = returned[['dates', 'LineDescription', 'METRIC_NAME', 'CL_UNIT', 'DataValue']]
        print(returned)

        
        returned = returned.pivot(index = "dates", columns = "LineDescription", values = "DataValue").reset_index(0)
        print(returned)

        return(returned)      

    #allows user to check the description and frequency of dataset 
    def check_freq(self, survey, table): 
        url = self.url + "&method=GetParameterValues&datasetname=" + survey + "&ParameterName=TableName&ResultFormat=JSON"
        tables  = self.format_json(url, "ParamValue")
        print(tables["Description"].loc[tables["TableName"] == table])

    #ask carmine how to make absract classes work in python
    #formats the json into a python dataframe
    def format_json(self, url, value):
        request = requests.get(url)
        request = request.json()

        data = request["BEAAPI"]["Results"].get(value)
        keys = list(data[0].keys())

        #pull data from json file that you need
        dict = {}
        for key in keys:
            dict[key] = [d.get(key) for d in data]
        returned = pd.DataFrame(dict)
        
        return(returned)
    
    def bea_tables(self, parameters):
        position = parameters.pop(0)
    
        bea_data = self.pulldata_table(position.get("survey"), position.get("table"), position.get("freq"))

        for parameter in parameters:
            second_data = self.pulldata_table(parameter.get("survey"), parameter.get("table"), parameter.get("freq"))
            bea_data = pd.merge(bea_data, second_data, how = "outer")
        
        return(bea_data)


#v1

#v2
class treasury:

    def __init__(self):
        self.v1 = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/"
        self.v2 = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/"
    
    def view(self):
        v1_endpoints = {"accounting": ["dts/operating_cash_balance", "dts/deposits_withdrawals_operating_cash", "dts/public_debt_transactions", 
                   "dts/public_debt_transactions_cash_basis", "dts/debt_subject_to_limit", "dts/inter_agency_tax_transfers",
                   "dts/income_tax_refunds_issued", "dts/federal_tax_deposits", "dts/short_term_cash_investments"]}

        v2_endpoints = {"accounting": ["od/avg_interest_rates", "od/debt_to_penny", "od/balance_sheets"], 
                        "debt" : ["","tror/collected_oustanding_recv", "tror/delinquent_debt", "tror/collections_delinquent_debt", "tror/written_off_delinquent_debt"]}      

        confirm = "T"
        while confirm == "T":   
            answer = input("Do you want to v1 endpoints(1) or v2 endpoints(2): ")

            if(answer == 1):
                dict = v1_endpoints
            elif(answer == 2):
                dict = v2_endpoints
            else:
                print("Restart program again")

            for key in dict.keys():
                eps = [key.title + item for item in dict[key]]
                print(eps)

    def format_json(self, part_1, part_2):
        request = requests.get(part_1 + part_2) 
        request = request.json()

        json_data = request["data"]
        keys = json_data[0].keys()

        treasury_data = {}
        for key in keys:
            treasury_data[key] = [item.get(key) for item in json_data]
        
        treasury_data = pd.DataFrame(treasury_data)

        

class main:

    pd.set_option('display.max_colwidth', None)

    #Remember to remove api keys

    #fred = FRED("565e55b6fbd720965afae15454629fae")   
    #fred_data = fred.collection(["DFF","DGS1MO","DGS3MO", "DGS6MO" ,"DGS1", "DGS2", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30"])      
    #print("Fred", fred_data)
    
    #bls = BLS("ee2f076f1d254305bc09f42aa498afab", ['CEU3000000001', 'CUUR0000SA0', 'LNU02000000', 'LNU03000000'])
    #print("BLS")
    #bls_data = bls.collection(1980, 2024, ["man_emp","cpi","employment", "unemp_lvl"])
    

    #bea = BEA("A2D2AB50-A251-4378-8CC1-95E51C78615E")
    #parameters = [{"survey" : "NIPA", "table" : "T20600", "freq" : "M"},
                  {"survey" : "NIPA", "table" : "T20306", "freq" : "Q"}]
    #bea_data = bea.bea_tables(parameters)
    #print("BEA", bea_data)
    
    
    #data = pd.merge(fred_data, bls_data, how = "left")
    #data = pd.merge(data, bea_data, how = "left")
    
    #print(data)   

    #treasury data

        
        
        
        
            
        

 