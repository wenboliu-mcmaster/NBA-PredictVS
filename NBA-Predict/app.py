import configparser
import mysql.connector
import logging
from NBA_Predict import predictDailyGames
from datetime import date
from getDailyMatchups import dailyMatchupsPresent
import pandas as pd 

class App(object):
    __instance=None 
    def setup(self):
                
        self.mydb = mysql.connector.connect(
            host = "b34wwyzhk6qwzgerwfb0-mysql.services.clever-cloud.com",
            user="uojlwsohujiqmbo1",
            passwd="db8jXOpiyuTYrlLryjHS",
            database="b34wwyzhk6qwzgerwfb0"
            )
    def _sql_Select_query(self):
        
        mycursor = self.mydb.cursor()
        stm=";"
        mycursor.execute(stm)
        myresult = mycursor.fetchall()
    def _sql_insert_query(self):
        
        mycursor = self.mydb.cursor()
        stm=";"
        mycursor.execute(stm)  
             
 
        

    def __new__(cls):
        if (cls.__instance is None):
            cls.__instance = super(App,cls).__new__(cls)
            cls.__instance.setup()
        return cls.__instance
    def daily_prediction_entry(self):
        today = date.today()
        # dd/mm/YY
        currentdate = today.strftime("%m/%d/%y")
        db_dailymatchups_results=[]
        
        db_dailymatchups_results=predictDailyGames(currentdate, '2021-22', '10/19/2021')
        homeTeam=[]
        awayTeam=[]
        winpercentage=[]
        winpercentage= db_dailymatchups_results[1][:,1]
        for k,v in db_dailymatchups_results[0].items():
            homeTeam.append(k) 
            awayTeam.append(v)
        #print(f"{homeTeam}=>{awayTeam}")
  
        for i in range ( len(homeTeam)):
            print(homeTeam[i]+awayTeam[i]+str(winpercentage[i]))
            with open('result.csv', 'w') as file: 
                file.write(homeTeam[i]+","+awayTeam[i]+","+str(winpercentage[i]))
                          

App().daily_prediction_entry()
