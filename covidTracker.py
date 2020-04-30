import json
import csv
import requests
from pymongo import MongoClient
import pprint
from bson.son import SON

def main():
    covidDataURL = 'https://covidtracking.com/api/v1/states/daily.json'
    statesDataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    credsFile = 'credentials.json'
    configFile = 'trackerConfig.json'
    db = getDB(credsFile)
    updateDB(db,covidDataURL, statesDataURL)
    config = configure(configFile)
    covid = db['covid']
    pipeline = [{"$match": {"state": "CA"}},
                {"$match": {"date": {"$gte": 20200401, "$lte": 20200415}}},
                {"$project": {"_id":0, "positive":1, "date":1}},
                {"$sort": {"date":1}}]
    pprint.pprint(list(db.covid.aggregate(pipeline)))

#updates db collections with data from APIs
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
    for line in csv.split("\n")[1:]:
        line = line.split(",")
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
