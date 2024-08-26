import pandas as pd
from bs4 import BeautifulSoup as bs
import time
from time import sleep
import selenium as sl
from selenium import webdriver as wb
from selenium.webdriver.firefox.options import Options 
from selenium.webdriver.common.by import By
import re

fox_options = Options()
fox_options.add_argument('--width=1000')
fox_options.add_argument('--height=915')

fire_fox = wb.Firefox()
fire_fox.get("https://stockanalysis.com/list/sp-500-stocks/")


fire_fox.implicitly_wait(45)
fire_fox.find_element(By.XPATH, '/html/body/div/div[2]/div/button').click()


response = fire_fox.find_element(By.XPATH, "/html/body/div/div[1]/div[2]/main/div/div/div/div[5]/table/tbody").text
print(response)
companies = response.split("\n")
#companies = soup.find_all('tr', {'class':'svelte-eurwtr'})
print(companies)

names_companies = {}
for company in companies:
    names_companies["ticker"] = re.
    names_comapnies[""]


#fire_fox.execute_script("window_scrollTo(0, document.body.scrollHeight)")
#count = 0
#information = []
#while(count != 49):
#    fire_fox.implicitly_wait(100)
#    fire_fox.execute_script("window.scrollBy(0, 430)")
    
#    item = fire_fox.find_element(By.XPATH, "/html/body/div/div[1]/div[2]/main/div/div/div/div[5]/table/tbody").text



#    information.append()
#    count += 1



#    print("Iteration: ", count)
    

#time.sleep(5)




#make list of html pages
#scrap pages for company names and tickers
#use the cik finder and get their ciks
#then get their 8Ks and 10Qs

#st_title = []
#title = fire_fox.find_element(By.CSS_SELECTOR,'.mb-0').text
#st_title.append(title)
#print(st_title)

fire_fox.quit()






