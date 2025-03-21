from pandas.tseries.offsets import DateOffset
from pandas import option_context
from itertools import chain
from datetime import date
import pandas as pd
import requests
import json
import math
import os

headers = {
    "User-Agent" : "Student M"
}

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

            print("V1 endpoints: ", v1_endpoints)
            print("\nV2 endpoints: ", v2_endpoints)

            if(answer == 1):
                dict = v1_endpoints
            elif(answer == 2):
                dict = v2_endpoints
            else:
                print("Restart program again")

            for key in dict.keys():
                eps = [key.title + item for item in dict[key]]
                print(eps)
    
    #gets the url given the version and endpoint
    def endpoints(self, version, endpoints):
        if(version == "1"):
            url = self.v1 + endpoints
        else:
            url = self.v2 + endpoints
        returned = self.format_json(url + "?sort=record_date&page[size]=10000&format=json")
        
        return returned 

    #collectinc data putting data 
    def collection(self, dict):
        #{version:[endpoints]}    
        #creates the tuple pairs with dictionary
        combos = [(k, v) for k, v in dict.items()]
        first = combos.pop(0)

        version = first[0]
        endpoint = first[1]

        final = self.endpoints(version, endpoint)
        
        #iterates through tuple pairs of versions and endpoints
        for unit in combos:
            second = self.endpoints(unit[0], unit[1])
            final = pd.merge(final, second, how = "outer")
        
        final = final.rename(columns = {"record_date":"dates"})
        final["dates"] = pd.to_datetime(final["dates"])

        print(final)
        final = final.drop([item for item in final.columns if item.startswith("record")], axis = 1)

        print(final)
        

        return final
    
    #captures the json for each dataset
    def format_json(self, url):
        request = requests.get(url, headers = headers) 
        request = request.json()

        json_data = request.get("data")
        keys = list(json_data.pop(0).keys())

        print(json_data)

        treasury_data = {}
        for key in keys:
            treasury_data[key] = [item.get(key) for item in json_data]
        
        treasury_data = pd.DataFrame(treasury_data)
        return treasury_data 


