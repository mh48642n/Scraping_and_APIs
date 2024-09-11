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
    
    def collection(self, parameters):
        position = parameters.pop(0)
    
        bea_data = self.pulldata_table(position.get("survey"), position.get("table"), position.get("freq"))

        for parameter in parameters:
            second_data = self.pulldata_table(parameter.get("survey"), parameter.get("table"), parameter.get("freq"))
            bea_data = pd.merge(bea_data, second_data, how = "outer")
        
        return(bea_data)
