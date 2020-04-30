import json
import csv
import requests
from pymongo import MongoClient
import pprint

def main():
    covidDataURL = 'https://covidtracking.com/api/v1/states/daily.json'
    statesDataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    credsFile = 'credentials.json'
    configFile = 'trackerConfig.json'
    db = getDB(credsFile)
    updateDB(db,covidDataURL, statesDataURL)
    config = configure(configFile)
    covidColl = db['covid']
    pprint.pprint(covidColl.find_one())

def updateDB(db, covidDataURL, statesDataURL):
    covidStr = requests.get(covidDataURL).text
    statesCSV = requests.get(statesDataURL).text
    covid = json.loads(covidStr)
    states = csv2json(statesCSV)
    db.covid.drop()
    db.states.drop()
    db.covid.insert_many(covid)
    db.states.insert_many(states)

def csv2json(csv):
    newLis = []
    header = csv.split("\n")[0].split(",")
    for line in csv.split("\n"):
        line = line.split(",")
        if line == header:
            pass
        else:
            dic = {}
            for item in range(len(header)):
                dic[header[item]] = line[item]
            newLis.append(dic)  
    return(newLis)

#authenticate MongoDB using credentials in credentials.json and return Client
def getDB(credentialFile):
    username = ""
    password = ""
    with open(credentialFile) as f:
        jsonData = json.loads("\n".join(f.readlines()))
        username = jsonData['username']
        password = jsonData['password']
        server = jsonData['server']
        authDB = jsonData['authDB']
        workDB = jsonData['db']
    client = MongoClient(server,
                    username=username,
                    password=password,
                    authSource=authDB,
                    authMechanism='SCRAM-SHA-256')
    db = client[workDB]
    return db  
 
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
