import configparser
from pickle import NONE
import mysql.connector
import logger,logging
from NBA_Predict import predictDailyGames
from datetime import date, timedelta,datetime
from getDailyMatchups import dailyMatchupsPresent,dailyMatchupsPast
import pandas as pd 
from teamIds import teams
import json 
#import _pickle as pickle
import csv
import requests
from datetime import datetime, timezone
from dateutil import parser
#from updatedailymatch import game_time
#from nba_api.live.nba.endpoints import scoreboard
#from nba_api.live.nba.endpoints import boxscore
import requests

class App(object):
    __instance=None 

    def setup(self):
        today = date.today()
        yesterday = today - timedelta(1)
        # dd/mm/YY
        self.currentdate = today.strftime("%m/%d/%y")
        self.yesterday=yesterday.strftime('%Y%m%d')
        
                
        self.mydb = mysql.connector.connect(
            host = "b34wwyzhk6qwzgerwfb0-mysql.services.clever-cloud.com",
            user="uojlwsohujiqmbo1",
            passwd="db8jXOpiyuTYrlLryjHS",
            database="b34wwyzhk6qwzgerwfb0"
            )
    def _sql_Select_query(self,stm,data):
        
        mycursor = self.mydb.cursor()
        
        mycursor.execute(stm,data)
    
        myresult = mycursor.fetchall()
        
        for x in myresult:
            #print(x)
             return x
    def _sql_insert_query(self):
        
        mycursor = self.mydb.cursor()
        stm="INSERT INTO `Team`(`TeamId`,`SportLeagueId`, `TeamName`, `City`, `AssetPath`) ""VALUES (%s,%s,%s,%s,%s)"
        mycursor.execute("SET foreign_key_checks = 0")
        mycursor.execute("DELETE FROM `Team` WHERE True")
        for i in range (len(teams.keys())):
            
            data = (i+1,1,list(teams.keys())[i],list(teams.keys())[i].split(" ")[0],"some/url")
           
            mycursor.execute(stm,data)  
            self.mydb.commit()
    def _sql_insert_game(self,data):
        mycursor = self.mydb.cursor()
        
        stm="INSERT INTO `Game`(`GameId`,`SeasonId`, `HomeTeamId`, `AwayTeamId`, `Date`, `StartTime`) ""VALUES (%s,%s,%s,%s,%s,%s)"   
        #mycursor.execute("DELETE FROM `Game` WHERE True")
        mycursor.execute(stm,data)   
    def _sql_insert_prediction(self,data):

        mycursor = self.mydb.cursor()
        #mycursor.execute("DELETE FROM `MLModelGamePrediction` WHERE True")
        stm="INSERT INTO `MLModelGamePrediction`(`MLModelId`, `GameId`, `HomeTeamPred`, `Percentage`) ""VALUES (%s,%s,%s,%s)"   
        mycursor.execute(stm,data)   
        #mycursor.execute("CALL AUTO_SELECT(id,hometeampreidct,1)") 
        self.mydb.commit()
    def _auto_slelect(self,id,hometeampredict):

        mycursor = self.mydb.cursor()
        #mycursor.execute("DELETE FROM `MLModelGamePrediction` WHERE True")
        #stm="INSERT INTO `MLModelGamePrediction`(`MLModelId`, `GameId`, `HomeTeamPred`, `Percentage`) ""VALUES (%s,%s,%s,%s)"   
        #mycursor.execute(stm,data)  
        args = [id, hometeampredict,1] 
        mycursor.callproc('AUTO_SELECT', args) 
        self.mydb.commit()
    def _sql_update_null(self):
        mycursor = self.mydb.cursor()
        stm="UPDATE `Game` SET `HomeTeamWon`=NULL "   
        mycursor.execute(stm)   
        self.mydb.commit()

    def __new__(cls):
        if (cls.__instance is None):
            cls.__instance = super(App,cls).__new__(cls)
            cls.__instance.setup()
        return cls.__instance
    def updata_yesterday_games(self):
        #url="https://api.sportsdata.io/v3/nba/scores/json/GamesByDate/2022-02-16?key=bd619c3787264f4fa32d0057a47e386a"
        url='http://data.nba.net/10s/prod/v1/'+self.yesterday+'/scoreboard.json'
        y=requests.get(url,timeout=120)
        numgames=y.json()["numGames"]
        vistor_score=[]
        home_score=[]
        home_win=[]
        mycursor = self.mydb.cursor()  
        yesterday=date.today() - timedelta(1)

        for i in range(numgames):    
            
            home_score.append(y.json()["games"][i]["hTeam"]['score'])
            vistor_score.append(y.json()["games"][i]["vTeam"]['score'])
            if int(home_score[i])>int(vistor_score[i]):
                home_win.append(1)
            else:
                home_win.append(0)
            #print(home_win)
            
            #print(vistor_score)
            #print(home_score[i]>vistor_score[i])
        for i in range (len(home_win)):

            stm= "UPDATE `Game` SET `HomeTeamWon`=%s WHERE  `GameId`=%s " 
            data=(home_win[i],yesterday.month*10000+yesterday.day*100+i+1)
            mycursor.execute(stm,data)   
            self.mydb.commit() 

    def daily_prediction_entry(self):
        
        db_dailymatchups_results= []
        try:
            db_dailymatchups_results=predictDailyGames(self.currentdate ,'2021-22', '10/19/2021')  
        except Exception as e:
            
            print(e)
            print('Because there is no game schedue for '+self.currentdate)
            logger.logger.error('there is no game schedue for '+self.currentdate)
            quit()
       
        homeTeam=[]
        awayTeam=[]
        winpercentage=[]
        url_today='http://data.nba.net/10s/prod/v1/'+date.today().strftime('%Y%m%d')+'/scoreboard.json'
        y=requests.get(url_today,timeout=120)
        numgames=y.json()["numGames"]
        homeStartTime=[]
        for i in range(numgames):   
            #homeStartTime.append(y.json()["games"][i]["startTimeEastern"].split(' ')[0].split(':')[0]*100+y.json()["games"][i]["startTimeEastern"].split(' ')[0].split(':')[1])
            homeStartTime.append(int(y.json()["games"][i]["startTimeEastern"].split(' ')[0].split(':')[0]+y.json()["games"][i]["startTimeEastern"].split(' ')[0].split(':')[1])+1200)
        
        winpercentage= db_dailymatchups_results[1][:,1]        
        for k,v in db_dailymatchups_results[0].items():
            homeTeam.append(k) 
            awayTeam.append(v)  
        with open('mysql_backup.txt', 'a') as f:

            for i in range ( len(homeTeam)):
                
                id=datetime.now().month*10000+datetime.now().day*100+i+1

                HomeTeamId=self._sql_Select_query("SELECT `TeamId`FROM `Team` WHERE  `TeamName`=%(name)s",{'name':homeTeam[i]})
                AwayTeamId=self._sql_Select_query("SELECT `TeamId`FROM `Team` WHERE  `TeamName`=%(name)s",{'name':awayTeam[i]})

            
                data_game = (id,1,HomeTeamId[0],AwayTeamId[0],date.today().strftime("%y-%m-%d"),homeStartTime[i])
           
                self._sql_insert_game(data_game)
                data_pred=(1,id,int(winpercentage[i]>0.5),f'{winpercentage[i]:.2f}')
                #input_data=(id,int(winpercentage[i]>0.5),1)
                self._sql_insert_prediction(data_pred)
                self._auto_slelect(id,int(winpercentage[i]>0.5))
                #self._sql_update_null()

                self.mydb.commit()                     
                print(homeTeam[i]+awayTeam[i]+str(int(winpercentage[i]>0.5)))        
            
        
                f.write("added on date:"+self.currentdate+homeTeam[i]+awayTeam[i]+'  '+str(winpercentage[i])+"\n")
        

 
      
#App()._sql_insert_query()
#print(dailyMatchupsPresent(App().currentdate)

App().daily_prediction_entry()
App().updata_yesterday_games()


