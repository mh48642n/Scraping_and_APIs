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

        path = r"C:/Users/marvi/OneDrive/Documents/GitHub/Data/public_debt/" + filename
        #path = filename
        print(path)

        
        if(os.path.exists(path)):
            

            #make function that passes to excel writer
            #accessing the original frame
            original = pd.read_csv(path)
            original["dates"] = pd.to_datetime(original["dates"], format = 'mixed')
            data["dates"] = pd.to_datetime(data["dates"], format = 'mixed') 

            original = original.dropna(subset = "dates")            
            length = len(original) 

            try:
                start = data['dates'].iloc[length]
            except IndexError:
                return
                
            print("Length - ", length)
            added = data.iloc[length:, ]
            print(added)

            #find the time frequency of data
            time_points = data[["dates"]].iloc[-3:-1, ].values
            print(time_points)
            
            # if(filename != "auction_data.csv"):
            result = self.time_difference(time_points, date.today(), start.date())
            
            if(result.get("Last") < result.get("Split")):
                print("Data not available...have to wait", result["Split"] ,"of time\n",
                        "Data is avaliable ", result["Frequency"])
                print("Data dropped ", result["Last"], " days ago")
                    
            else:
                self.appending(path, added, length)
                self.additional(path)
        
        else:
            #creates new excel page
            data.to_csv(path)
    


    #FIX THIS IT
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
        

    #access the file first
    #check the dates of the data on the file
    #subset the data based off the dates found on the file
    #find the blank cell in A column and then insert
    #actually append data to sheet
    #save sheet afterwards
    def appending(self, path, data, length):
        print("Running")

        data = data.reset_index()
        cols = data.columns.tolist()
        #    data = data.rename(columns = {'index':'Unnamed: 0'})
        #elif '' in data.columns.tolist():
        #     data = data.rename(columns = {'':'Unnamed:0'})
        # else:
        #    pass
        
        
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

    def additional(self, path):
        check = pd.read_csv(path)
        check = check.dropna(subset = ["dates"])
        check.to_csv(path, index = False)



        # wb = op.load_workbook(path)

        # #wb = load_workbook(path)
        # writer = pd.ExcelWriter(path, engine = "openpyxl")
        # writer.book = wb

        # writer.sheets = dict((ws.title, ws) for ws in wb.worksheets)

        # data.to_excel(writer, "Sheet1")
        # writer.save()

        

        #ws = wb.active
        
        # with pd.ExcelWriter(path, engine = "openpyxl", mode = "a", if_sheet_exists = "error") as writer: 
        #    data.to_excel(writer, sheet_name = "Sheet1", index = False)
           
            # for i in range(0, len(data)):
            #     insert = data.iloc[i].values.tolist()
            #     ws.append(insert)
            #     row_num = insert[0]

            


        #    ws.append(insert)

        #wb.save(path)




        #add_to_og = data.to_dict()
        #print(add_to_og)

        #wb = load_workbook(path)
        #ws = wb.active

        #ws.append(add_to_og)
        #ws.save(path)
        #print("Pulled data\n", data)

        #appending_part = data[data["dates"] > start]

    

class main:

    os.chdir("C:/Users/marvi/OneDrive/Documents/GitHub/Data/public_debt")

    keys = {
        fred: [{"series":["DFF","DGS1MO","DGS3MO", "DGS6MO" ,"DGS1", "DGS2", "DGS5", "DGS7", "DGS10", "TREAST"], 
                 "filename": "yields_datasets.csv"},
                {"series":["THREEFYTP1", "THREEFYTP2", "THREEFYTP5", "THREEFYTP7", "THREEFYTP10"],
                  "filename": "fred_term_premiums.csv"}],
        bls:  [{"series":[['CUUR0000SA0', 'LNU02000000', 'LNU03000000'], 1980, 2024, ["CPI", "employment", "unemployment"]]
                 , "filename": "bls_datasets.csv"}],
        bea:  [{"series":[{"survey" : "NIPA", "table" : "T20600", "freq" : "M"},
                           {"survey" : "NIPA", "table" : "T20306", "freq" : "Q"}], 
                 "filename": "bea_datasets.csv"}],
        treasury:  [{"series":{"2":"accounting/od/debt_to_penny"}, "filename": "public_debt.csv"},
                    {"series":{"1":"accounting/od/auctions_query"}, "filename": "auction_data.csv"}]
        }


    dc = collection_data(keys)
    dc.getting_data()
    


    







