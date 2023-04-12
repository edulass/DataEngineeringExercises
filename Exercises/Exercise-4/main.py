import os
import json
import pandas as pd

#recursive function to find all the existing folders
#will be used to look for json files in all existing folders
def AllFoldersPaths(directory_path):
        
    all_files = os.listdir(directory_path)
    FoldersList=[]
    for filename in all_files:
        if os.path.isdir(os.path.join(directory_path,filename)):
            subfolders = AllFoldersPaths(os.path.join(directory_path,filename))
            FoldersList.extend(subfolders)
            FoldersList.append(os.path.join(directory_path,filename))
    return FoldersList
    

    
def main():
    
    #Get a list of all the folders with the root folder andall the folders found with the recursive function
    FoldersList=["/app/data/"]
    FoldersList+=AllFoldersPaths("/app/data/")
    
    #Put in a list the path of all the found json files
    json_files=[]
    for folder in FoldersList:
        all_files = os.listdir(folder)
        for filename in all_files:
            if filename.endswith('.json'):
                json_files.append(os.path.join(folder,filename))
            
    #Convert and aggregate all the json files into a single csv file
    aux=0    
    with open("/app/AggregatedJsons.csv","w") as f:
        for jsonfile in json_files:
            with open(jsonfile,'r') as f1:
                data=json.load(f1)
            if aux==0:
                for keys in data.keys():
                    if keys=='geolocation':
                        f.write('Geolocation x coordinate,Geolocation y coordinate\n')
                    else:
                        f.write('{},'.format(keys))
    
            for keys in data.keys():
                if keys=='geolocation':
                    f.write('{},{}\n'.format(data[keys]['coordinates'][0],data[keys]['coordinates'][1]))
                else:
                    f.write('{},'.format(data[keys]))
    
            aux+=1
        
    
   
if __name__ == '__main__':
    main()
