import requests
import pandas as pd
import urllib.request
import os
from bs4 import BeautifulSoup


def main():
    
    page=requests.get("https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/")
    soup = BeautifulSoup(page.content, 'html.parser')
    index=0
    for item in soup.find_all("td"):
        
        if '2022-02-07 14:03' in str(item):
            
            SubHTML=str(list(soup.find_all("td"))[index-1]) 
            break
        index+=1
    
    csv=SubHTML[13:28]
    
    urllib.request.urlretrieve("https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"+csv, '/app/weather.csv')
    
    Weather=pd.read_csv('/app/weather.csv')
    
    MaxIndex=Weather["HourlyDryBulbTemperature"].idxmax()
    MaxNumber=Weather["HourlyDryBulbTemperature"].max()
    
    print(Weather.loc[[MaxIndex]])
    
if __name__ == '__main__':
    main()
