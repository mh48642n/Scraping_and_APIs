import pandas as pd
import time
from time import sleep
import selenium #Need this step 
from selenium import webdriver #Need this step 

from selenium.webdriver.common.by import By #Allows for selenium to click things 
from selenium.webdriver.firefox.options import Options as Firefox_options


options = Firefox_options()

corps = []

try:
    driver = webdriver.Firefox()
    driver.get("https://stockanalysis.com/list/sp-500-stocks/")

    time.sleep(30)
    driver.find_element(By.XPATH, "/html/body/div/div[2]/div/button").click()
    driver.find_element(By.XPATH,   "/html/body/div/div[1]/div[2]/main/h1")

    #use beautiful soup and grab the td's and search the slw
    rows = driver.find_elements(By.CLASS_NAME, "svelte-eurwtr")

    for row in rows:
        driver.implicitly_wait(15)
        corp = row.find_element(By.CLASS_NAME, "slw svelte-eurwtr").text
        corps.append(corp)
        #print(row.text)
    #/html/body/div/div[1]/div[2]/main/div/div/div/div[4]/table/tbody/tr[2]/td[3]
    #tr.svelte-eurwtr:nth-child(3) > td:nth-child(3)
    #tr.svelte-eurwtr:nth-child(2) > td:nth-child(3)
finally:
    driver.quit()

print(corps)