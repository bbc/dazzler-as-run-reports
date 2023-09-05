import pandas as pd

def getSheet(name):
    return  pd.read_excel(name)
