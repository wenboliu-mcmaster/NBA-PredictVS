import requests
from datetime import datetime, timezone
homeTeam=[]
awayTeam=[]
winpercentage=[]
url_today='http://data.nba.net/10s/prod/v1/'+datetime.today().strftime('%Y%m%d')+'/scoreboard.json'
y=requests.get(url_today,timeout=120)
numgames=y.json()["numGames"]
homeStartTime=[]
for i in range(numgames): 
    homeStartTime=[]
    homeStartTime.append(int(y.json()["games"][i]["startTimeEastern"].split(' ')[0].split(':')[0]+y.json()["games"][i]["startTimeEastern"].split(' ')[0].split(':')[1])+1200)
print(homeStartTime)       
print("102">"97")