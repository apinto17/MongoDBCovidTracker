import json
import csv
import requests


def main():
    stateDataURL = 'https://covidtracking.com/api/v1/states/daily.json'
    countyDataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    updateFiles(stateDataURL, countyDataURL)



def updateFiles(state, county):
    r = requests.get(state)
    with open('daily.json','w') as f:
        f.write(r.text)
    r = requests.get(county)
    with open('us-counties.csv','w') as f:
        f.write(r.text)
    print("Local Files Updated")    




if ('__name__' == '__main__'):
    main()                
