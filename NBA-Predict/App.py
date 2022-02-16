import configparser
from pickle import NONE
import mysql.connector
import logging
from NBA_Predict import predictDailyGames
from datetime import date
from getDailyMatchups import dailyMatchupsPresent,dailyMatchupsPast
import pandas as pd 
from teamIds import teams
import json 
import _pickle as pickle

class App(object):
    __instance=None 

    def setup(self):
        today = date.today()
        # dd/mm/YY
        self.currentdate = today.strftime("%m/%d/%y")
                
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
        
        stm="INSERT INTO `Game`(`GameId`,`SeasonId`, `HomeTeamId`, `AwayTeamId`, `Date`, `StartTime`, `HomeTeamWon`) ""VALUES (%s,%s,%s,%s,%s,%s,%s)"   
        mycursor.execute("DELETE FROM `Game` WHERE True")
        mycursor.execute(stm,data)   
    def _sql_insert_prediction(self,data):

        mycursor = self.mydb.cursor()
        mycursor.execute("DELETE FROM `MLModelGamePrediction` WHERE True")
        stm="INSERT INTO `MLModelGamePrediction`(`MLModelId`, `GameId`, `HomeTeamPred`, `Percentage`) ""VALUES (%s,%s,%s,%s)"   
        mycursor.execute(stm,data)         

    def __new__(cls):
        if (cls.__instance is None):
            cls.__instance = super(App,cls).__new__(cls)
            cls.__instance.setup()
        return cls.__instance
    def daily_prediction_entry(self):

        db_dailymatchups_results= []
        
        db_dailymatchups_results=predictDailyGames(self.currentdate, '2021-22', '10/19/2021')
        homeTeam=[]
        awayTeam=[]
        winpercentage=[]
        winpercentage= db_dailymatchups_results[1][:,1]
        for k,v in db_dailymatchups_results[0].items():
            homeTeam.append(k) 
            awayTeam.append(v)
        
       
        for i in range ( len(homeTeam)):

            HomeTeamId=self._sql_Select_query("SELECT `TeamId`FROM `Team` WHERE  `TeamName`=%(name)s",{'name':homeTeam[i]})
            AwayTeamId=self._sql_Select_query("SELECT `TeamId`FROM `Team` WHERE  `TeamName`=%(name)s",{'name':awayTeam[i]})
            #print(type(HomeTeamId)+type(AwayTeamId))
            data_game = (i,1,HomeTeamId[0],AwayTeamId[0],date.today().strftime("%y-%m-%d"),"1900",str(int(winpercentage[i]>0.5))) 
           
            self._sql_insert_game(data_game)
            data_pred=(1,i,int(winpercentage[i]>0.5),f'{winpercentage[i]:.2f}')
            self._sql_insert_prediction(data_pred)
            self.mydb.commit() 
                       
             
            #print(homeTeam[i]+awayTeam[i]+str(int(winpercentage[i]>0.5)))
            with open('result.csv', 'w') as file: 
                
                file.write("hometeam:  "+str(homeTeam[i])+" "+"awayteam:  "+str(awayTeam[i])+self.currentdate+" hometeamwin:  "+str(int(winpercentage[i]>0.5)))
                file.write("added on date"+" "+self.currentdate)
        
        
      
App()._sql_insert_query()
#print(dailyMatchupsPresent(App().currentdate)

App().daily_prediction_entry()


