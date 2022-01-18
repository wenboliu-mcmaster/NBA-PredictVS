# createModel.py - Used to train, test, and create the model
# Call createModel() to generate a new model
# May need to edit which lines are commented out based on what range of game data you would like to use

from standardizeStats import basicOrAdvancedStatZScore, basicOrAdvancedStatStandardDeviation, basicOrAdvancedStatMean
from getDailyMatchups import dailyMatchupsPast
from getStats import getStatsForTeam
from availableStats import availableStats
from configureCWD import setCurrentWorkingDirectory

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
import pandas as pd
import pickle

from datetime import timedelta, date


# Calculates the zScore differential between two teams for a specified stat
def zScoreDifferential(observedStatHome, observedStatAway, mean, standardDeviation):

    homeTeamZScore = basicOrAdvancedStatZScore(observedStatHome, mean, standardDeviation)
    awayTeamZScore = basicOrAdvancedStatZScore(observedStatAway, mean, standardDeviation)

    differenceInZScore = homeTeamZScore - awayTeamZScore
    return differenceInZScore


# Used to combine and format all the data to be put into a pandas dataframe
# dailyGames should be list where index 0 is a dictionary holding the games and index 1 is a list holding the results
def infoToDataFrame(dailyGames, meanDict, standardDeviationDict, startDate, endDate, season):

    fullDataFrame = []
    gameNumber = 0  # Counter to match the result of the game with the correct game
    dailyResults = dailyGames[1]  # List of results for the games

    for homeTeam,awayTeam in dailyGames[0].items():

        homeTeamStats = getStatsForTeam(homeTeam, startDate, endDate, season)
        awayTeamStats = getStatsForTeam(awayTeam, startDate, endDate, season)

        currentGame = [homeTeam,awayTeam]

        for stat,statType in availableStats.items():  # Finds Z Score Dif for stats listed above and adds them to list
            zScoreDif = zScoreDifferential(homeTeamStats[stat], awayTeamStats[stat], meanDict[stat], standardDeviationDict[stat])
            currentGame.append(zScoreDif)

        if dailyResults[gameNumber] == 'W':  # Sets result to 1 if a win
            result = 1
        else:  # Sets result to 0 if loss
            result = 0

        currentGame.append(result)
        gameNumber += 1

        print(currentGame)
        fullDataFrame.append(currentGame)  # Adds this list to list of all games on specified date

    return(fullDataFrame)


# Function that allows iterating through specified start date to end date
def daterange(startDate, endDate):

    for n in range(int ((endDate - startDate).days)):
        yield startDate + timedelta(n)


# Returns a list. Index 0 is a dict holding mean for each stat. Index 1 is a dict holding standard deviation for each stat.
def createMeanStandardDeviationDicts(startDate, endDate, season):

    meanDict = {}
    standardDeviationDict = {}

    # Loops through and inputs standard deviation and mean for each stat into dict
    for stat, statType in availableStats.items():
        statMean = basicOrAdvancedStatMean(startDate, endDate, stat, statType, season)
        meanDict.update({stat: statMean})

        statStandardDeviation = basicOrAdvancedStatStandardDeviation(startDate, endDate, stat, statType, season)
        standardDeviationDict.update({stat: statStandardDeviation})

    bothDicts = []
    bothDicts.append(meanDict)
    bothDicts.append(standardDeviationDict)

    return bothDicts


# Loops through every date between start and end and appends each game to a singular list to be returned
# season should be in format 'yyyy-yy' and startOfSeason should be in format 'mm/dd/yyyy'
def getTrainingSet(startYear, startMonth, startDay, endYear, endMonth, endDay, season, startOfSeason):

    startDate = date(startYear, startMonth, startDay)
    endDate = date(endYear, endMonth, endDay)

    startDateFormatted = startDate.strftime("%m/%d/%Y")  # Formats start date in mm/dd/yyyy
    allGames = []

    for singleDate in daterange(startDate, endDate):
        currentDate = singleDate.strftime("%m/%d/%Y")  # Formats current date in mm/dd/yyyy
        print(currentDate)

        previousDay = singleDate - timedelta(days=1)
        previousDayFormatted = previousDay.strftime("%m/%d/%Y")

        meanAndStandardDeviationDicts = createMeanStandardDeviationDicts(startOfSeason, previousDayFormatted, season)
        meanDict = meanAndStandardDeviationDicts[0]  # Dict in format {stat:statMean}
        standardDeviationDict = meanAndStandardDeviationDicts[1]  # Dict in format {stat:statStDev}

        currentDayGames = dailyMatchupsPast(currentDate, season)  # Finds games on current date in loop
        currentDayGamesAndStatsList = infoToDataFrame(currentDayGames, meanDict, standardDeviationDict, startOfSeason, previousDayFormatted, season)  # Formats Z Score difs for games on current date in loop

        for game in currentDayGamesAndStatsList:  # Adds game with stats to list of all games
            game.append(currentDate)
            allGames.append(game)

    print(allGames)
    return(allGames)


# Returns a dataframe from list of games with z score differentials
def createDataFrame(listOfGames):

    games = pd.DataFrame(
        listOfGames,
        columns=['Home', 'Away', 'W_PCT', 'REB', 'TOV', 'PLUS_MINUS', 'OFF_RATING', 'DEF_RATING', 'TS_PCT', 'Result', 'Date']
    )

    print(games)
    return(games)


# Creates the logistic regression model and tests accuracy
def performLogReg(dataframe):

    # Update if new stats are added
    featureColumns = ['W_PCT', 'REB', 'TOV', 'PLUS_MINUS', 'OFF_RATING', 'DEF_RATING', 'TS_PCT']

    X = dataframe[featureColumns] # Features
    Y = dataframe.Result  # Target Variable

    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.25, shuffle=True)
    logreg = LogisticRegression()

    logreg.fit(X_train, Y_train)  # Fits model with data

    Y_pred = logreg.predict(X_test)

    confusionMatrix = metrics.confusion_matrix(Y_test, Y_pred)  # Diagonals tell you correct predictions

    # Code below prints model accuracy information
    print('Coefficient Information:')

    for i in range(len(featureColumns)):  # Prints each feature next to its corresponding coefficient in the model

        logregCoefficients = logreg.coef_

        currentFeature = featureColumns[i]
        currentCoefficient = logregCoefficients[0][i]

        print(currentFeature + ': ' + str(currentCoefficient))

    print('----------------------------------')

    print("Accuracy:", metrics.accuracy_score(Y_test, Y_pred))
    print("Precision:", metrics.precision_score(Y_test, Y_pred))
    print("Recall:", metrics.recall_score(Y_test, Y_pred))

    print('----------------------------------')

    print('Confusion Matrix:')
    print(confusionMatrix)

    return logreg


# Saves the model in folder to be used in future
# filename should be end in '.pkl'
def saveModel(model, filename):

    # Change to where you want to save the model
    setCurrentWorkingDirectory('SavedModels')

    with open(filename, 'wb') as file:
        pickle.dump(model, file)


# Used to generate new logistic regression models
# Can import the statistics and predictions for each game from a csv file or can be created on their own
def createModel(startYear=None, startMonth=None, startDay=None, endYear=None, endMonth=None, endDay=None, season='2018-19', startOfSeason = '10/16/2018', filename='model.pkl'):

    # allGames = getTrainingSet(startYear, startMonth, startDay, endYear, endMonth, endDay, season, startOfSeason)  # Unnecessary if using data from CSV file

    # allGamesDataframe = createDataFrame(allGames)  # Unnecessary if using data from CSV file

    setCurrentWorkingDirectory('Data')
    allGamesDataframe = pd.read_csv('COMBINEDgamesWithInfo2016-19.csv')  # Should be commented out if needing to obtain data on different range of games

    logRegModel = performLogReg(allGamesDataframe)

    saveModel(logRegModel, filename)
