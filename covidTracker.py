import json
import csv
import requests
from pymongo import MongoClient
import pprint
from bson.son import SON
from datetime import date, timedelta
import datetime as dt
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
    output = [{"date": 20200501, "ratio": 0.04109670512668015, "state": "CA"},
     {"date": 20200501, "ratio": 0.06036054152584703, "state": "NY"},
     {"date": 20200501, "ratio": 0.05681580233126265, "state": "WA"},
      {"date": 20200501, "ratio": 0.05681580233126265, "state": "MI"},
     {"date": 20200502, "ratio": 0.041592428683640825, "state": "CA"},
     {"date": 20200502, "ratio": 0.06041658013208638, "state": "NY"},
     {"date": 20200502, "ratio": 0.05629568900731024, "state": "WA"},
      {"date": 20200502, "ratio": 0.05681580233126265, "state": "MI"},
     {"date": 20200503, "ratio": 0.041312294837361985, "state": "CA"},
     {"date": 20200503, "ratio": 0.06064503895200923, "state": "NY"},
     {"date": 20200503, "ratio": 0.05532226887955742, "state": "WA"},
     {"date": 20200503, "ratio": 0.05532226887955742, "state": "MI"},
     {"date": 20200504, "ratio": 0.04102881482425324, "state": "CA"},
     {"date": 20200504, "ratio": 0.060871037425576806, "state": "NY"},
     {"date": 20200504, "ratio": 0.054922621007573266, "state": "WA"},
     {"date": 20200504, "ratio": 0.054922621007573266, "state": "MI"},
     {"date": 20200505, "ratio": 0.04121895680637586, "state": "CA"},
     {"date": 20200505, "ratio": 0.06116279359386286, "state": "NY"},
     {"date": 20200505, "ratio": 0.05439141120165567, "state": "WA"},
     {"date": 20200505, "ratio": 0.05439141120165567, "state": "MI"},
     {"date": 20200506, "ratio": 0.04100994644223412, "state": "CA"},
     {"date": 20200506, "ratio": 0.061352931371883274, "state": "NY"},
     {"date": 20200506, "ratio": 0.05527767089906374, "state": "WA"},
     {"date": 20200506, "ratio": 0.05527767089906374, "state": "MI"},
     {"date": 20200507, "ratio": 0.041310588312931, "state": "CA"},
     {"date": 20200507, "ratio": 0.06356802553952554, "state": "NY"},
     {"date": 20200507, "ratio": 0.05469977994341402, "state": "WA"},
     {"date": 20200507, "ratio": 0.05469977994341402, "state": "MI"},
     {"date": 20200508, "ratio": 0.041352060404402355, "state": "CA"},
     {"date": 20200508, "ratio": 0.06369417112833566, "state": "NY"},
     {"date": 20200508, "ratio": 0.054894954100178674, "state": "WA"},
     {"date": 20200508, "ratio": 0.054894954100178674, "state": "MI"}]
    db = getDB(credsFile)
    refresh(config['refresh'], db,covidDataURL, statesDataURL)
    pipelines = generate_pipeline(config)

    for pipeline in pipelines:
        print(pipeline)
        if config['collection'] == 'states':
            output = (list(db.states.aggregate(pipeline)))
        else:
            output = (list(db.covid.aggregate(pipeline)))
    print(output)
    # db = getDB(credsFile)
    # refresh(config['refresh'], db,covidDataURL, statesDataURL)
    # pipelines = generate_pipeline(config)
    # for pipeline in pipelines:
    #     if config['collection'] == 'states':
    #         output = (list(db.states.aggregate(pipeline)))
    #     else:
    #         output = (list(db.covid.aggregate(pipeline)))
    # print(output)
    interpret_output(config, output)
        
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
        pipe3 = None
        for to_do in task['task']:
            level = config['aggregation']
            if level == 'fiftyStates' or level == 'usa': 
                if to_do == "track":    
                    pipe = {"$group":{"_id":"$date", task['task']['track']:{"$sum":"$" + task['task']['track']}}}
                    pipe2 = {"$project":{"_id":0, "date": "$_id", task['task']['track'] : 1}}
                if to_do == "ratio":
                    pipe = {"$group":{"_id":"$date", task['task']['ratio']['numerator']:{"$sum":"$" + task['task']['ratio']['numerator']}, task['task']['ratio']['denominator']:{"$sum":"$" + task['task']['ratio']['denominator']}}}
                    pipe2 = {"$match" : { task['task']['ratio']['denominator']: {"$ne": 0}}} 
                    pipe3 = {"$project":{"_id":0, "date":"$_id", "ratio":{"$divide": ["$" + task['task']['ratio']['numerator'], "$" + task['task']['ratio']['denominator']]}}}
                if to_do == "stats":
                    to_do_stats = []
                    for var in task['task']['stats']:
                        varDic = str('"avg' + var + '" : ' + ' {"$avg" : "$' + var + '"}, "std' + var + '" : {"$stdDevPop" : "$' + var + '"}') 
                        to_do_stats.append(varDic)
                    to_project = json.loads("{"+ ", ".join(to_do_stats) + "}" ) .keys()
                    project = '"' + '": 1, "'.join(to_project) + '": 1'
                    pipe = json.loads('{"$group" : {"_id": "$date" , ' + ', '.join(to_do_stats) + '}}' )
                    pipe2 = json.loads('{"$project":{"_id":0, "date":"$_id", ' + project + '}}')
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
                if to_do == "track":    
                    pipe = {"$project":{"_id":0,"county":1, "date":1, task['task']['track']:1}}
                if to_do == "ratio":
                    pipe = {"$match" : { task['task']['ratio']['denominator']: {"$ne": 0}}} 
                    pipe2 = {"$project":{"_id":0,"county":1, "date":1, "ratio":{"$divide": ["$" + task['task']['ratio']['numerator'], "$" + task['task']['ratio']['denominator']]}}}
                if to_do == "stats":
                    to_do_stats = []
                    for var in task['task']['stats']:
                        varDic = str('"avg' + var + '" : ' + ' {"$avg" : "$' + var + '"}, "std' + var + '" : {"$stdDevPop" : "$' + var + '"}') 
                        to_do_stats.append(varDic)
                    to_project = json.loads("{"+ ", ".join(to_do_stats) + "}" ) .keys()
                    project = '"' + '": 1, "'.join(to_project) + '": 1'
                    pipe = json.loads('{"$group" : {"_id": "$county" , ' + ', '.join(to_do_stats) + '}}' )
                    pipe2 = json.loads('{"$project":{"_id":0, "county":"$_id", ' + project + '}}')
        if pipe3:
            tasks.append(str(pipe) +"---"+ str(pipe2) + "---" + str(pipe3))
        elif pipe2:
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


def interpret_output(config, output):
    for task in config["analysis"]:
        if("graph" in task["output"].keys()):
            make_graph(task, output, config)
        if("table" in task["output"].keys()):
            make_table(task)


def make_table(task):
    pass


def make_graph(task, output, config):
    graph = task["output"]["graph"]
    output_var = get_output_var(task["task"])
    if(graph["type"] == "bar"):
        render_plots(output, output_var, config, graph, plt.bar)
    elif(graph["type"] == "line"):
        render_plots(output, output_var, config, graph, plt.plot)
    elif(graph["type"] == "scatter"):
        render_plots(output, output_var, config, graph, plt.scatter)


def render_plots(output, output_var, config, graph, plot_func):
    target = None
    if("target" in config.keys() and (config["aggregation"] == "state" or config["aggregation"] == "county")):
        target = config["target"]
    xlist, ylist = report_xy_lists(output, output_var, target)
    if(graph["combo"] == "separate"):
        make_plots(xlist, ylist, config, graph, output_var, False, plot_func)
        plt.show()
    elif(graph["combo"] == "combine"):
        make_plots(xlist, ylist, config, graph, output_var, True, plot_func)
        plt.show()
    elif(graph["combo"] == "split"):
        xlist_of_3, ylist_of_3 = get_lists_of_3(xlist, ylist)
        i = 0
        offset = 0
        while(i < len(xlist_of_3)):
            plt.figure(i)
            make_plots(xlist_of_3[i], ylist_of_3[i], config, graph, output_var, True, plot_func, offset)
            offset += len(xlist_of_3[i])
            i += 1
        plt.show()


def get_lists_of_3(xlist, ylist):
    xlist_of_3 = []
    ylist_of_3 = []
    i = 0
    while(i < len(xlist)):
        j = 0
        xTemp = []
        yTemp = []
        while(i < len(xlist) and j < len(xlist) and j < 3):
            xTemp.append(xlist[i])
            yTemp.append(ylist[i])
            i += 1
            j += 1
        xlist_of_3.append(xTemp)
        ylist_of_3.append(yTemp)
    
    return xlist_of_3, ylist_of_3


def make_plots(xlist, ylist, config, graph, output_var, combine, plot_func, offset=0):
    for i in range(len(xlist)):
        legend_name = None
        if("legend" in graph.keys() and graph["legend"] == "on"):
            legend_name = config["target"][i + offset]
        if(combine):
            plot_xy(plot_func, xlist[i], ylist[i], graph, output_var, legend_name)
        else:
            plot_xy(plot_func, xlist[i], ylist[i], graph, output_var, legend_name, i)
        if("legend" in graph.keys() and graph["legend"] == "on"):
            plt.legend()


def report_xy_lists(output, output_var, targets):
    xlist = []
    ylist = []

    if(type(targets) is list):
        target_type = get_target_type(output)
        for target in targets:
            x, y = report_xy(output, output_var, target, target_type)
            xlist.append(x)
            ylist.append(y)
    else:
        x, y = report_xy(output, output_var)

    xlist = [x]
    ylist = [y]

    return xlist, ylist


def get_target_type(output):
    target_type = None
    if("state" in output[0].keys()):
        target_type = "state"
    elif("county" in output[0].keys()):
        target_type = "county"
    else:
        raise ValueError("Target type cannot be found")
    
    return target_type


def plot_xy(plot_func, x, y, graph, output_var, legend_name, figure_num=None):
    if(figure_num is not None):
        plt.figure(figure_num)
    if("title" in graph.keys()):
        plt.suptitle(graph["title"])
    if(legend_name is not None):
        plot_func(x, y, label=legend_name)
    else:
        plot_func(x, y)


def get_output_var(task):
    if("ratio" in task.keys()):
        return "ratio"
    elif("stats" in task.keys()):
        return "stats"
    else:
        return task["track"]


def report_xy(output, output_var, target=None, target_type=None):
    x = []
    y = []
    for data_point in output:
        if(target is None):
            x.append(to_datetime(data_point["date"]))
            y.append(data_point[output_var])
        elif(data_point[target_type] == target):
            x.append(to_datetime(data_point["date"]))
            y.append(data_point[output_var])
    return x, y


def to_datetime(time):
    time_str = str(time)
    year = int(time_str[0:4])
    month = int(time_str[4:6])
    day = int(time_str[6:])
    return date(year, month, day)


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
    state_conv = {'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC', 'Marshall Islands': 'MH', 'Armed Forces Africa': 'AE', 'Armed Forces Americas': 'AA', 'Armed Forces Canada': 'AE', 'Armed Forces Europe': 'AE', 'Armed Forces Middle East': 'AE', 'Armed Forces Pacific': 'AP', 'Puerto Rico': 'PR', 'Virgin Islands': 'VI', 'Guam': 'GU', 'Northern Mariana Islands': 'MP'}
    newLis = []
    header = csv.split("\n")[0].split(",")
    tracker = {}
    for line in csv.split("\n")[1:]:
        line = line.split(",")
        dic = {}
        today = line[0].split("-")
        yesterday = (date(int(today[0]), int(today[1]), int(today[2])) - timedelta(days = 1)).strftime('%Y%m%d')
        death_prev = 0
        pos_prev = 0
        if (state_conv[line[2]] + line[1] + yesterday) in tracker.keys():
            death_prev = tracker[state_conv[line[2]] + line[1] + yesterday][1]
            pos_prev = tracker[state_conv[line[2]] + line[1] + yesterday][0]
        dic = {'date' : int("".join(line[0].split("-"))), 'county' : line[1], 'state' : state_conv[line[2]], 'positive' : int(line[4]), 'death' : int(line[5]), 'deathIncrease' : int(line[5]) - death_prev, 'positiveIncrease' : int(line[4]) - pos_prev}
        tracker[dic['state'] +dic['county'] + str(dic['date'])] = [dic['positive'], dic['death']]
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
