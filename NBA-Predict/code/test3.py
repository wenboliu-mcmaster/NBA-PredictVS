# Query nba.live.endpoints.scoreboard and  list games in localTimeZone
from datetime import datetime, timezone
from dateutil import parser
from nba_api.live.nba.endpoints import scoreboard
from nba_api.live.nba.endpoints import boxscore
import requests
# another free and lightweight api using json to achieve is :
#For a list of all the json files that NBA releases for each day:

# http://data.nba.net/10s/prod/v1/today.json

# This list will provide links to other relevant information including:

# scores and stats for games of the day: http://data.nba.net/10s/prod/v1/20170218/scoreboard.json

# player information: http://data.nba.net/10s/prod/v1/2016/players.json

# all star roster (for this weekend): http://data.nba.net/10s/prod/v1/allstar/2016/AS_roster.json

# player profile: (note: you need to look up the player id in order to get the player’s stat)

# /data/10s/prod/v1/2016/players/{{personId}}_profile.json

# and more including team rosters, schedules, standings, etc.

# accessing PlayerProfileV2 endpoints
# waiting too long..
#x = requests.get('https://stats.nba.com/stats/playerprofilev2?LeagueID=00&PerMode=Totals&PlayerID=1629637')
#print(x.status_code)
from datetime import date, timedelta
yesterday_date = date.today() - timedelta(1)
url_today='http://data.nba.net/10s/prod/v1/'+yesterday_date.strftime('%Y%m%d')+'/scoreboard.json'
url='https://nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData/scoreboard/todaysScoreboard_00.json'
url2='https://stats.nba.com/stats/boxscoreplayertrackv2?GameID=0021700807'
y=requests.get(url_today,timeout=120)
numgames=y.json()["numGames"]

vistor_score=[]
home_score=[]
home_win=[]
for i in range(numgames):    
    vistor_score.append(y.json()["games"][i]["vTeam"]['score'])
    home_score.append(y.json()["games"][i]["hTeam"]['score'])
    if home_score[i]>vistor_score[i]:
        home_win.append(1)
    else:
        home_win.append(0)
    
print(home_win)    


box = boxscore.BoxScore('0022100766') 

players = box.away_team.get_dict()['players']
f = "{player_id}: {name}: {points} PTS"
# for player in players:
#     print(f.format(player_id=player['personId'],name=player['name'],points=player['statistics']['points']))
# f = "{gameId}: {awayTeam} vs. {homeTeam} @ {gameTimeLTZ}" 

board = scoreboard.ScoreBoard()
#print("ScoreBoardDate: " + board.score_board_date)
games = board.games.get_dict()
startTime=[]
startTime2=[]

def game_time():
    
    for game in games:
        gameTimeLTZ = parser.parse(game["gameTimeUTC"]).replace(tzinfo=timezone.utc).astimezone(tz=None)
        #print(f.format(gameId=game['gameId'], awayTeam=game['awayTeam']['teamName'], homeTeam=game['homeTeam']['teamName'], gameTimeLTZ=gameTimeLTZ))
        startTime.append(str(gameTimeLTZ).split(' ')[1].split('-')[0].split(':')[:-1])
        #startTime2.append(int(startTime[0]+startTime[1]))
    #print(startTime)
    for i in range (len(startTime)):
        startTime2.append(int(startTime[i][0]+startTime[i][1]))
    #print(startTime2)
    
    
    return startTime2

a=game_time()
print(a)
with open('game_time.txt', 'a') as f:
    f.write(str(a)+"\n")

    