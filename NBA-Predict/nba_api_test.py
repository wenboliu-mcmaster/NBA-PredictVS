# Query nba.live.endpoints.scoreboard and  list games in localTimeZone
from datetime import datetime, timezone
from dateutil import parser
from nba_api.live.nba.endpoints import scoreboard
from nba_api.live.nba.endpoints import boxscore
import requests
from requests.exceptions import HTTPError
# accessing PlayerProfileV2 endpoints
# waiting too long..
x = 'https://stats.nba.com/stats/playerprofilev2?LeagueID=00&PerMode=Totals&PlayerID=1629637&Format=json'
url='https://nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData/scoreboard/todaysScoreboard_00.json'
url2="http://nba.cloud/games/0022000180/boxscore?Format=json"

url3='https://stats.nba.com/stats/boxscoreplayertrackv2?GameID=0021700807'
y=requests.get(url,timeout=120)
print(y.json())



box = boxscore.BoxScore('0022100766') 

players = box.away_team.get_dict()['players']
f = "{player_id}: {name}: {points} PTS"
for player in players:
    print(f.format(player_id=player['personId'],name=player['name'],points=player['statistics']['points']))
f = "{gameId}: {awayTeam} vs. {homeTeam} @ {gameTimeLTZ}" 

board = scoreboard.ScoreBoard()
print("ScoreBoardDate: " + board.score_board_date)
games = board.games.get_dict()

for game in games:
    gameTimeLTZ = parser.parse(game["gameTimeUTC"]).replace(tzinfo=timezone.utc).astimezone(tz=None)
    print(f.format(gameId=game['gameId'], awayTeam=game['awayTeam']['teamName'], homeTeam=game['homeTeam']['teamName'], gameTimeLTZ=gameTimeLTZ))