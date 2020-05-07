import json
import csv
import requests
from pymongo import MongoClient
import pprint
from bson.son import SON
from datetime import date, timedelta
import matplotlib.pyplot as plt
import sys

def main():
    covidDataURL = 'https://covidtracking.com/api/v1/states/daily.json'
    statesDataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    credsFile = 'credentials.json'
    configFile = 'trackerConfig.json'
    if len(sys.argv) == 5:
        credsFile = sys.argv[2]
        configFile = sys.argv[4]
    if len(sys.argv) == 3:
        if sys.argv[1] == "-auth":
            credsFile = sys.argv[2]
        if sys.argv[1] == "-config":    
            configFile = sys.argv[2]
    config = configure(configFile)
    db = getDB(credsFile)
    refresh(config['refresh'], db,covidDataURL, statesDataURL)
    pipelines = generate_pipeline(config)
    for pipeline in pipelines:
        print("pipeline: ", pipeline)
       # if config['collection'] == 'states':
       #     pprint.pprint(list(db.states.aggregate(pipeline)))
       # else:
       #     pprint.pprint(list(db.covid.aggregate(pipeline)))
        
#this is where the real work is. Takes in the config file and creates a pipeline based on the contents of said file.
def generate_pipeline(config):
    pipelines = []
    keys = config.keys()
    tasks = interpret_aggregate(config)
    for task in tasks:
        pipeline = []
        if 'counties' in keys and interpret_counties(config):
            pipeline.append(interpret_counties(config))
        if 'target' in keys and interpret_target(config):
            pipeline.append(interpret_target(config))
        if 'time' in keys and interpret_time(config):
            pipeline.append(interpret_time(config))
        if 'aggregation' in keys and config['aggregation'] == 'fiftyStates':
            fifty_states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
            pipeline.append({"$match": {"state": {"$in": fifty_states}}})
        if (isinstance(task, str)):
            for t in task.split('---'):
                a = '"'.join(t.split("'"))
                pipeline.append(json.loads(a))
        else:
            pipeline.append(task)
        pipeline.append({"$sort":{"date": 1, "state": 1}})
        pipelines.append(pipeline)
    return pipelines


#each needs to take in the parameter from the config file, and return the appropriate monogDB query.
#example output for one of the functions would be "{"$match": {"state": "CA"}}" 

def interpret_time(config):
    time = config['time']
    today = date.today()
    pipe = ""
    if time == "today":
        pipe = {"$match":{"date": int(today.strftime('%Y%m%d'))}}
    elif time == "yesterday":
        yesterday = today - timedelta(days=1)
        pipe = {"$match":{"date":int(yesterday.strftime('%Y%m%d'))}}
    elif time == "week":
        week_ago = today - timedelta(weeks=1)
        pipe = {"$match":{"date":{"$gte":int(week_ago.strftime('%Y%m%d')),  "$lte":int(today.strftime('%Y%m%d'))}}}
    elif time == "month":
        pipe = {"$match":{"date":{"$gte":int(today.strftime('%Y%m01')), "$lte":int(today.strftime('%Y%m%d'))}}}
    else:
        start = time['start']
        end = time['end']
        pipe = {"$match":{"date":{"$gte":int(start) , "$lte":int(end)}}}
    return pipe  


def interpret_aggregate(config):
    pipe = ""
    tasks = []
    for task in config['analysis']:
        pipe2 = None
        for to_do in task['task']:
            level = config['aggregation']
            if level == 'fiftyStates' or level == 'usa': 
                pipe = {"$group":{"_id":"$date", task['task']['track']:{"$sum":"$" + task['task']['track']}}}
                pipe2 = {"$project":{"_id":0, "date":"$_id", task['task']['track'] : 1}}
            elif level == 'state':
                if to_do == "track":    
                    pipe = {"$project":{"_id":0,"state":1, "date":1, task['task']['track']:1}}
                if to_do == "ratio":
                    pipe = {"$match" : { task['task']['ratio']['denominator']: {"$ne": 0}}} 
                    pipe2 = {"$project":{"_id":0,"state":1, "date":1, "ratio":{"$divide": ["$" + task['task']['ratio']['numerator'], "$" + task['task']['ratio']['denominator']]}}}
                if to_do == "stats":
                    to_do_stats = []
                    for var in task['task']['stats']:
                        varDic = str('"avg' + var + '" : ' + ' {"$avg" : "$' + var + '"}, "std' + var + '" : {"$stdDevPop" : "$' + var + '"}') 
                        to_do_stats.append(varDic)
                    to_project = json.loads("{"+ ", ".join(to_do_stats) + "}" ) .keys()
                    project = '"' + '": 1, "'.join(to_project) + '": 1'
                    pipe = json.loads('{"$group" : {"_id": "$state" , ' + ', '.join(to_do_stats) + '}}' )
                    pipe2 = json.loads('{"$project":{"_id":0, "state":"$_id", ' + project + '}}')
            elif level == 'county': 
                pipe = {"$group":{"_id":"$county", task['task']['track']:{"$sum":"$" + task['task']['track']}}}
                pipe2 = {"$project":{"_id":0, "county":"$_id", task['task']['track'] : 1}}
        if pipe2:
            tasks.append(str(pipe) +"---"+ str(pipe2))
        else: 
            tasks.append(pipe)
    return tasks


def interpret_target(config):
    pipe = ""
    states = config["target"]
    if(config["collection"] == "states"):
        pipe = {"$match": {"state": states}}
    else:
        if(type(states) is list):
            pipe = {"$match": {"state": {"$in": states}}} 
        else:
            pipe = {"$match": {"state": states}}
    return pipe


def interpret_counties(config):
    if(config["collection"] == "states"):
        counties = config["counties"]
        if(type(counties) is list):
            return {"$match": {"county": {"$in": counties }}} 
        else:
            return {"$match": {"county": counties}}
        
    else:
        return ""


def interpret_output(config):
    for task in config["analysis"]:
        if("graph" in task["output"].keys()):
            graph = task["output"]["graph"]
            if(graph["type"] == "bar"):
                plt.bar()
            elif(graph["type"] == "line"):
                plt.plot()
            elif(graph["type"] == "scatter"):
                plt.scatter()


#updates db collections with data from APIs
def refresh(refresh_bool, db, covidDataURL, statesDataURL):
    collections = db.list_collection_names()
    if refresh_bool or (not 'states' in collections) or (not 'covid' in collections):
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
