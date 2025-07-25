from data_apis import macroeconomic_apis as ms
from data_apis import treasury_api as tr
from data_apis import bls_api as bs
from data_apis import bea_api as ba
import pandas as pd
import os
import openpyxl as op
import time
#from dateutil import  relative
import csv
from datetime import *
import datetime


# calls text file with api keys
with open(r"C:\Users\marvi\OneDrive\Documents\text_files_for_api\APIs.txt", 'r') as file:
    api_keys = file.readlines()    

# creates empty dictionary
lock = {}

# inserting api keys for each api associated to it
for item in api_keys:
    key = item[:4]
    val = item[6:-2]

    lock[key] = val
    

fred = ms.FRED(lock['FRED'])
bls = bs.BLS(lock['BLS '])
bea = ba.BEA(lock['BEA '])
treasury = tr.treasury()

class collection_data:

    def __init__(self, keys):
        self.keys = keys
        

    #Executes the process of getting the data 
    #after collecting data check if file for it exist
    def getting_data(self):
        keys_stuff = self.keys

        for key in keys_stuff.keys():
            values = self.keys.get(key)
            for value in values:
                print(value.get("series"))
                data = key.collection(value.get("series"))
                self.file_name_check(value.get("filename"), data)
        


    #check if file exists and then opens it or creates it
    def file_name_check(self, filename, data):
        
        #print(data)

        path = r"C:/Users/marvi/OneDrive/Documents/GitHub/Data/macro_datasets/" + filename
        #path = filename
        print(path)

        #finds if the path 
        if(os.path.exists(path)):
            

            # pulls original dataset to compare to pulled data to find add new piece
            original = pd.read_csv(path)
            original["dates"] = pd.to_datetime(original["dates"], format = 'mixed')
            data["dates"] = pd.to_datetime(data["dates"], format = 'mixed') 

            # gets the length of the original dataset to compare to pulled data
            original = original.dropna(subset = "dates")            
            length = len(original) 

            # handles index error when the data is examined to determine recent date
            try:
                start = data['dates'].iloc[length]
            except IndexError:
                return
                
            # Prints length of original and then locates the part to be appended to original
            print("Length - ", length)
            added = data.iloc[length:, ]
            print(added)

            # find the time frequency of data
            time_points = data[["dates"]].iloc[-3:-1, ].values
            print(time_points)
            
            # determines the distance from previous appending
            # auction dataset is not consistent so one should proxy for that
            result = self.time_difference(time_points, date.today(), start.date())
            
            if(result.get("Last") < result.get("Split")):
                print("Data not available...have to wait", result["Split"] ,"of time\n",
                        "Data is avaliable ", result["Frequency"])
                print("Data dropped ", result["Last"], " days ago")
                    
            else:
                # actual appending
                self.appending(path, added, length)
                self.additional(path)
        
        else:
            #creates new excel page
            data.to_csv(path)
    


    #determines the level of split 
    def time_difference(self, dates, day, start):
        
        dates = dates.tolist()
        last_pulled = day - start
        
        split = dates[1][0] - dates[0][0]
        split = split/86400000000000
        print("Split in diff", split)

        if split > 360:
            frequency = "annually"
        elif split > 90:
            frequency = "quarterly"
        elif(split > 27.00) & (split <= 31.00):
            frequency = "monthly"
        else:
            frequency = "daily"
        
        return {"Last": last_pulled.days, "Frequency" : frequency, "Split": split}
        

    #appending to dataset
    def appending(self, path, data, length):
        print("Running")

        data = data.reset_index()
        cols = data.columns.tolist()
        
        with open(path, "r") as csv_file:
            reader = csv.reader(csv_file)
            for header in reader:
                print(header)
                break
        
        for i in range(0, len(header)):
            data = data.rename(columns = {cols[i] : header[i]})

        data = data.to_dict("records")
        print("Length: ", length)
        print("Printing")


        with open(path, "a", newline = '') as csvfile:

            csvfile.seek(length + 2)
            writer = csv.DictWriter(csvfile, fieldnames = header)   
            print("Boom: ", csvfile.tell())
            writer.writerows(data)
    

            csvfile.close()

    #handles the na's between the appended and original
    def additional(self, path):
        check = pd.read_csv(path)
        check = check.dropna(subset = ["dates"])
        check.to_csv(path, index = False)

#Initiates the pulling process here and points the datasets to specific place
class main:

    os.chdir("C:/Users/marvi/OneDrive/Documents/GitHub/Data/macro_datasets")

    keys = {
        # FRED has the total amount of treasury securities held by the FED and
        # Yields dataset contain treasury yields for below maturities:
        # 1 month, 3 month, 6 month, 1 year, 2 year, 5 year, 7 year and 10 year maturities
        # There is also the treasury premium dataset estimated by Kim and Wright's 2005 paper
        fred: [{"series":["DFF","DGS1MO","DGS3MO", "DGS6MO" ,"DGS1", "DGS2", "DGS5", "DGS7", "DGS10", "TREAST"], 
                 "filename": "yields_datasets.csv"},
                {"series":["THREEFYTP1", "THREEFYTP2", "THREEFYTP5", "THREEFYTP7", "THREEFYTP10"],
                  "filename": "fred_term_premiums.csv"},
                {"series":["FDHBPIN", "FYGFDPUN", "GFDEBTN", "FDHBFIN"],
                 "filename": "public_debt_fred.csv"},
                {"series":["THREEFYTP1", "THREEFYTP2", "THREEFYTP5", "THREEFYTP7", "THREEFYTP10"],
                  "filename": "fred_term_premiums.csv"}],
          
        # # BLS has the CPI, Employment and Unemployment numbers
        bls:  [{"series":[['CUSR0000SA0', 'LNS12000000', 'LNS13000000'], 1964, 2024, 
                          ["CPI", "employment", "unemployment"]], 
                          "filename": "cpi_employment.csv"},
                {"series":[['CES0500000006', 'CES0500000007', 'CES0500000008', 'CES9091000001'], 1964, 2024, 
                        ["private_total_workers", "private_average_worked_hours", "private_average_hourly_wages", "federal_employees", ]], 
                        "filename": "workers_data.csv"}],
        # BEA has
        #     T20600 - Personal Income and its Disposition
        #     T10105 - Gross Domestic Product nominal
        #     T30100 - Receipts of government expenditures
        #     T50305 - Private Fixed Investment
        #     T50805 - Private Final Demand 
        #     T61600 - Coporate Income tables 
        bea:  
               [{"series": [{"survey" : "NIPA", "table" : "T20600", "freq" : "M"}], 
                 "filename": "personal_income.csv"},
               {"series" : [{"survey" : "NIPA", "table" : "T10105", "freq" : "Q"}], 
                 "filename" : "bea_gdp_govt.csv"},
               {"series" : [{"survey" : "NIPA", "table" : "T30200", "freq" : "Q"}], 
                 "filename" : "govt_receipts.csv"},
               {"series" : [{"survey" : "NIPA", "table" : "T50305", "freq" : "Q"}],
                 "filename" : "private_fixed_investment.csv"},
               {"series" : [{"survey" : "NIPA", "table" : "T50805A", "freq" : "Q"},
                            {"survey" : "NIPA", "table" : "T50805B", "freq" : "Q"}],
                "filename" : "private_final_demand.csv"},
               {"series" : [{"survey" : "NIPA", "table" : "T61600B", "freq" : "Q"}],
                "filename" : "corporate_income.csv"},
               {"series" : [{"survey" : "NIPA", "table" : "T61600C", "freq" : "Q"}],
                "filename" : "corporate_incomeA.csv"},
               {"series" : [{"survey" : "NIPA", "table" : "T61600D", "freq" : "Q"}],
                "filename" : "corporate_incomeB.csv"}, 
               {"series" : [{"survey" : "NIPA", "table" : "T30905", "freq" : "Q"}],
                "filename" : "govt_spending_decomp.csv"},
               {"series" : [{"survey" : "NIPA", "table" : "T31005", "freq" : "Q"}],
                "filename" : "govt_spending_output.csv"}],

        #Treasury includes the auctioned treasuries and public debt to the penny
         treasury:  [#{"series":{"2":"accounting/od/debt_to_penny"}, "filename": "public_debt.csv"},
                     {"series":{"1":"accounting/od/auctions_query"}, "filename": "auction_data.csv"}]
         }


    dc = collection_data(keys)
    dc.getting_data()
    

#['CUUR0000SA0', 'LNU02000000', 'LNU03000000', 'LEU0252881500','CES0500000007', 'CES0500000008']
    







