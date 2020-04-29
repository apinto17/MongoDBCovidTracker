import json
import csv
import requests
from pymongo import MongoClient

def main():
    stateDataURL = 'https://covidtracking.com/api/v1/states/daily.json'
    countyDataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    credsFile = 'credentials.json'
    configFile = 'trackerConfig.json'
    updateFiles(stateDataURL, countyDataURL)
    authenticate(credsFile)
    config = configure(configFile)
   
#authenticate MongoDB using credentials in credentials.json
def authenticate(credentialFile):
    with open(credentialFile) as f:
        jsonData = json.loads("\n".join(f.readlines()))
        username = jsonData['username']
        password = jsonData['password']
    
#get the task configuration from trackConfig.json
def configure(configFile):
    with open(configFile) as f:
        jsonData = json.loads("\n".join(f.readlines()))
    return jsonData

#updates local files with data from APIs
def updateFiles(state, county):
    r = requests.get(state)
    with open('daily.json','w') as f:
        f.write(r.text)
    r = requests.get(county)
    with open('us-counties.csv','w') as f:
        f.write(r.text)
    print("Local Files Updated")    



if __name__ == "__main__":
    main()                
