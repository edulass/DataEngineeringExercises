import requests
import os
import urllib.request
from zipfile import ZipFile




download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]

    


def main():

   #Create downloads folder if it doesn't exists
   if not os.path.exists(r"downloads"):
        os.makedirs(r"downloads")
        
   #Download the files and add try/except in case there are not downloadable url
   for url in download_uris:
    
        r = requests.get(url, stream=True)

        try:
            r.headers['content-length']
            urllib.request.urlretrieve(url, '/app/downloads/'+url[-23:])
        except:
            print("Error downloading "+ url)
   
   FileList=os.listdir("/app/downloads/")
   
   #Unzip
   for Zip in FileList:
   
        try:
            
            with ZipFile("/app/downloads/"+Zip, 'r') as zObject:
            
                 #Extracting all the members of the zip 
                 #into a specific location.
            
                 zObject.extractall(path="/app/downloads/")
                    
        except:
            print("Error unziping "+ zip)
               

   
   FileList=os.listdir("/app/downloads/")
   
   #delete all zip files
   for file in FileList:
        if ".zip" in file:
            os.remove("/app/downloads/"+file)
        
        
if __name__ == '__main__':
    main()
