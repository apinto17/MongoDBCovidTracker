import json
import csv
import requests
from pymongo import MongoClient
import pprint
from bson.son import SON
from datetime import date, timedelta

def main():
    covidDataURL = 'https://covidtracking.com/api/v1/states/daily.json'
    statesDataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    credsFile = 'credentials.json'
    configFile = 'trackerConfig.json'
    config = configure(configFile)
    db = getDB(credsFile)
    refresh(config['refresh'], db,covidDataURL, statesDataURL)
    pipeline = generate_pipeline(config)
    print(pipeline)
    if config['collection'] == 'states':
        pprint.pprint(list(db.states.aggregate(pipeline)))
    else:
        pprint.pprint(list(db.covid.aggregate(pipeline)))
        
#this is real the real work is. Takes in the config file and creates a pipeline based on the contents of said file.
def generate_pipeline(config):
    pipeline = []
    pipeline = [interpret_counties(config) , interpret_time(config)]
    #example pipeline to make sure everything is working
    #pipeline = [{"$match": {"state": "CA"}},{"$match": {"date": {"$gte": 20200401, "$lte": 20200415}}},{"$project": {"_id":0, "positive":1, "date":1}},{"$sort": {"date":1}}]
    return pipeline


#each needs to take in the parameter from the config file, and return the appropriate monogDB query.
#example output for one of the functions would be "{"$match": {"state": "CA"}}" 
def interpret_collection(collection):
    pass

def interpret_time(config):
    time = config['time']
    today = date.today()
    pipe = ""
    if time == "today":
        pipe = {"$match":{"date": today.strftime('%Y%m%d')}}
    elif time == "yesterday":
        yesterday = today - timedelta(days=1)
        pipe = {"$match":{"date":yesterday.strftime('%Y%m%d')}}
    elif time == "week":
        week_ago = today - timedelta(weeks=1)
        pipe = {"$match":{"date":{"$gte":week_ago.strftime('%Y%m%d'),  "$lte":today.strftime('%Y%m%d')}}}
    elif time == "month":
        pipe = {"$match":{"date":{"$gte":today.strftime('%Y%m01'), "$lte":today.strftime('%Y%m%d')}}}
    else:
        start = time['start']
        end = time['end']
        pipe = {"$match":{"date":{"$gte":start , "$lte":end}}}
    return pipe  


    

def interpret_target(target):
    pass


def interpret_counties(config):
    if(config["collection"] == "states"):
        counties = config["counties"]
        if(type(counties) is list):
            return {"$match": {"county": {"$in": counties }}} 
        else:
            return {"$match": {"county": counties}}
    else:
        return ""

def interpret_analysis(analysis):
    pass



#updates db collections with data from APIs
def refresh(refresh_bool, db, covidDataURL, statesDataURL):
    if refresh_bool:
        covidStr = requests.get(covidDataURL).text
        statesCSV = requests.get(statesDataURL).text
        covid = json.loads(covidStr)
        states = csv2json(statesCSV)
        db.covid.drop()
        db.states.drop()
        db.covid.insert_many(covid)
        db.states.insert_many(states)
    else:
        pass

def csv2json(csv):
    newLis = []
    header = csv.split("\n")[0].split(",")
    for line in csv.split("\n")[1:]:
        line = line.split(",")
        dic = {}
        for item in range(len(header)):
            if item == 0:
                dic[header[item]] = "".join(line[0].split("-"))
            else:
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
