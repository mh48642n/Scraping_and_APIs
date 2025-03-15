import wrds
import pandas as pd

db = wrds.Connection(wrds_username = "mh48642n")
confirm = "False"


while confirm != "True":
    choice = input("Choose libraries(L), tables(T), grab information(G): ")
    if(choice == "L"):
        print(db.list_libraries())
    elif(choice == "T"):
        library = input("Library Name:")
        print(db.list_tables(library = library))
    else:
        library = input("Library and table(comma separated): ")
        library = library.split()
        columns = list(input("Name: "))
        info = db.get_table(library = library[0], table = library[1], columns = columns)
    confirm = input("Stop?(True/False): ")
   

