from data_apis import macroeconomic_apis as ms
from data_apis import treasury_api as tr
from data_apis import bls_api as bs
from data_apis import bea_api as ba
import pandas as pd
import os
from openpyxl import load_workbook


fred = ms.FRED("565e55b6fbd720965afae15454629fae")
bls = bs.BLS("ee2f076f1d254305bc09f42aa498afab", 1980, 2024, ["CPI", "employment", "unemployment"])
bea = ba.BEA("A2D2AB50-A251-4378-8CC1-95E51C78615E")
treasury = tr.treasury()


class collection_data:

    def __init__(self, keys):
        self.keys = keys
        

    #Executes the process of getting the data 
    #after collecting data check if file for it exist
    def getting_data(self):
        
        for key in self.keys:
            values = self.keys.get(key)
            for value in values:
                data = key.collection(value.get("series"))
                self.file_name_check(value.get("filename"), data)
        


    #check if file exists and then opens it or creates it
    def file_name_check(self, filename, object):
        path = "C:/Users/marvi/OneDrive/Documents/GitHub/Data/public_debt/" + filename + ".xlsx"
        if(os.path.exists(path)):
            #make function that passes to excel writer
            self.appending(path, object)
        else:
            object.to_excel(path)

    #access the file first
    #check the dates of the data on the file
    #subset the data based off the dates found on the file
    #actually append data to sheet
    #save sheet afterwards
    def appending(self, path, data):
        original = pd.read_excel(path)
        start = original["dates"].max()

        appending_part = data.loc[data["dates"] > start]
        original = pd.concat([original, appending_part], ignore_index = True)
    

class main:

    
    #fred_data = fred.collection(["DFF","DGS1MO","DGS3MO", "DGS6MO" ,"DGS1", "DGS2", "DGS5", "DGS7", "DGS10", "TREAST"])

    keys = {
        #fred: [{"series":["DFF","DGS1MO","DGS3MO", "DGS6MO" ,"DGS1", "DGS2", "DGS5", "DGS7", "DGS10", "TREAST"], 
        #       "filename": "yields_datasets"},
        #       {"series":["THREEFYTP1", "THREEFYTP2", "THREEFYTP5", "THREEFYTP7", "THREEFYTP10"],
        #        "filename": "fred_term_premiums"}],
        bls:  [{"series":['CUUR0000SA0', 'LNU02000000', 'LNU03000000'], "filename": "bls_datasets"}],
        bea:  [{"series":[{"survey" : "NIPA", "table" : "T20600", "freq" : "M"},
                {"survey" : "NIPA", "table" : "T20306", "freq" : "Q"}], 
                "filename": "bea_datasets"}],
        #treasury:  [{"series":{"2":"accounting/od/debt_to_penny"}, "filename": "public_debt"}, 
        #            {"series":{"1":"accounting/od/auctions_query"}, "filename": "auction_data"}]
    }

    dc = collection_data(keys)
    dc.getting_data()
    
    #bls_data = bls.collection()

    #fred_data = fred.collection(["DFF","DGS1MO","DGS3MO", "DGS6MO" ,"DGS1", "DGS2", "DGS5", "DGS7", "DGS10", "TREAST"])

    #bea_data = bea.collection([{"survey" : "NIPA", "table" : "T20600", "freq" : "M"},
    #                {"survey" : "NIPA", "table" : "T20306", "freq" : "Q"}])

    
    #treasury_data = treasury.collection({"2":"accounting/od/debt_to_penny"})

    #data = pd.merge(fred_data, bls_data, how = "left")
    #data_list = [bea_data, treasury_data]

    #for item in data_list:
    #    data = pd.merge(data, item, how = "left")

    #print(data)

    #data.to_excel("C:/Users/marvi/OneDrive/Documents/GitHub/Data/public_debt/master_thesis_data.xlsx")








