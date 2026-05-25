import pandas as pd
import requests as rq

### Intents: Instead passing table and surveys to the class how about
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
            case 1:

                #pulled = self.format_json("&method=GETDATASETLIST&ResultFormat=JSON", pull_this)

                # getting requests for all the available surveys
                pull = rq.get((self.url + "&method=GETDATASETLIST&ResultFormat=JSON"), headers = header)
                pull = pull.json()["BEAAPI"]["Results"]["Dataset"]

                for i in range(0, len(pulled)):
                    print(pull[i]["DatasetName"] + " - " + pull[i]["DatasetDescription"])

            # pulls tables in a survey
            case 2:

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
            case 3:  
                self.macro_data(json_table)


    def macro_data(self, json_object, year = "All", freq = "Q", tables = None):
        
        # Pulling all tables for the first time
        if(year == "All" & tables == None):

            # constructs dictionary of tables and the url parameters
            # this will collect the data objects for all tables listed in the JSON 
            # still need to add a way to include the year parameter
            return {k : self.build_url(json_object[k]) for k in json_object.keys()}
                
        elif(year != "All" | tables != None):

            # this will collect data objects for all the tables listed in the tables object
            return {k : self.build_url(json_object[k]) for k in tables}
                      
                
    def build_url(self, parameters):

        # checks if this a list of parameters instead of just a string
        # if it is a string it skips to the return statement outside of the function
        if isinstance(parameters, list):

            # this puts the parameters into the parts of the url
            url = "&method=GetData&DataSetName=" + parameters[0] + "&TableName=" + parameters[2] + "&tableID=ALL&Frequency=" + parameters[1] + "&Year=ALL&ResultFormat=JSON"

            # returns the results of the json pull with the specific parameters included 
            return self.format_json(url)
        # returns json pull given the specific url string
        return self.format_json(parameters)
        

    def format_json(self, url_end, select):
        
        total_url = self.url + url_end

        if(select == 1):

        elif():
        else:
            # extracts the data from the API
            pull = rq.get(total_url, headers = header)

            # takes the note if it is there
            try:
                note = pull.json()["BEAAPI"]["Results"]["Notes"][0]['NoteText']
            except KeyError:
                print("No notetext for Table ", pull.json()["BEAAPI"]["Results"]["Dataset"][0].get("TableName"),
                    ", the type of table is a ", pull.json()["BEAAPI"]["Results"]["Dataset"].get("Statistic"))

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