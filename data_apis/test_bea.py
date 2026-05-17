import datetime 
import pandas as pd
import requests as rq

### Instead passing table and surveys to the class how about
### I create an textfile to have the table, surveys and frequency
header = {
    "User-Agent" : "Student Mars"
}

class BEA:

    def __init__(self, key):
        self.url = "http://apps.bea.gov/api/data?&UserID=" + key

    def data_pull(self, pull_this, *json_table):

        match(pull_this):     

            # pull survey names
            case "Surveys":

                # getting requests for all the available surveys
                pull = rq.get((self.url + "&method=GETDATASETLIST&ResultFormat=JSON"), headers = header)
                pull = pull.json()["BEAAPI"]["Results"]["Dataset"]

                for i in range(0, len(pull)):
                    print(pull[i]["DatasetName"] + " - " + pull[i]["DatasetDescription"])

            # pulls tables in a survey
            case "Tables":

                # getting requests from the particular survey
                pull = rq.get((self.url + "&method=GetParameterValues&datasetname=" + input("What survey?.....") + 
                                "&ParameterName=TableName&ResultFormat=JSON"), headers = header)
                pull = pull.json()["BEAAPI"]["Results"]["ParamValue"]

                confirm = "True"

                while confirm == "True":    
                    
                    print("This is where we can scroll through the available tables")
                    range_vals = input("Input a range of values as XXX-XXX(59-150, 1-10): ").split("-")

                    for i in range(int(range_vals[0]), int(range_vals[1])):
                        print(pull["TableName"][i], "....", pull["TableDescription"][i])

                    confirm = input("Do you want to look at more tables(True or False):")

            # pulls table 
            case "Table Data":  
                self.macro_data(json_table)


    def macro_data(self, json_object):
        
        
        
        
        return True              
                

    def format_json(self, rest_url):

        # extracts the data from the API
        pull = rq.get((self.url + rest_url), headers = header)

        # takes the note if it is there
        try:
            note = pull.json()["BEAAPI"]["Results"]["Notes"][0]['NoteText']
        except KeyError:
            note = "Nothing"

        # converts the API into json then into a dataframe getting a table
        data = pd.DataFrame(pull.json()["BEAAPI"]["Results"]["Dataset"])

        return self.format_data(data, note)
    
        
    def format_data(self, table, note):
        
        # subsets the data by economic account
        description = table["LineDescription"].unique()
        
        # creates an empty dataframe
        concat = pd.DataFrame()        

        # takes the list of economic accounts and iterates with it
        for item in description:

            # creates a dataframe with that economic account name column, data value and date
            df = table[table["LineDescription"] == item]

            # pivots that dataframe around the date, with the focus of creating a column
            # with the economic account's name and it the associated data value with the date
            # it is made for
            df = df.pivot_table(index = "dates", columns = "LineDescription", values = "DataValue", aggfunc = 'first')
            
            # concats the new column into the empty dataframe and then adds on each new
            # column
            concat = pd.concat([concat, df], axis = 1)  

        # returns the full table with all the economic accounts as columns 
        return concat.reset_index(drop = False)
    
    
