import datetime 
import pandas as pd
import requests as rq

class BLS:

    def __init__(self, key):
        self.header = {'Content-type' : 'application/json'}
        self.key = key

    def 
