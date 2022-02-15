import mysql.connector


mydb = mysql.connector.connect(
    host = "b34wwyzhk6qwzgerwfb0-mysql.services.clever-cloud.com",
    user="uojlwsohujiqmbo1",
    passwd="db8jXOpiyuTYrlLryjHS",
    database="b34wwyzhk6qwzgerwfb0"
)
print(mydb)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM User")
myresult = mycursor.fetchall()
for x in myresult:
    print(x)
print("\n")

mycursor.execute("SELECT * FROM Team")
myresult = mycursor.fetchall()
for x in myresult:
    print(x)
print("\n")

mycursor.execute("SELECT * FROM Game")
myresult = mycursor.fetchall()
for x in myresult:
    print(x)
print("\n")

# Selects games with team names now showing
mycursor.execute("SELECT Game.*, ht.TeamName as homeTeam, at.TeamName as awayTeam FROM Game LEFT JOIN Team ht ON Game.HomeTeamId = ht.TeamId LEFT JOIN Team at ON Game.AwayTeamId = at.TeamId")
myresult = mycursor.fetchall()
for x in myresult:
    print(x)
print("\n")

# Shows all users in pool 1
mycursor.execute("SELECT PoolMembership.*, User.Nickname FROM PoolMembership LEFT JOIN User ON PoolMembership.UserId = User.UserId  WHERE PoolId = 1")
myresult = mycursor.fetchall()
for x in myresult:
    print(x)
print("\n")

# Shows all predictions made in pool 1
mycursor.execute("SELECT pm.PoolId, pm.UserId, g.GameId, pmp.HomeTeamPred, g.HomeTeamWon FROM PoolMemberPrediction pmp INNER JOIN PoolMembership pm ON pmp.MembershipId = pm.MembershipId INNER JOIN Game g ON pmp.GameId = g.GameId WHERE PoolId = 1")
myresult = mycursor.fetchall()
for x in myresult:
    print(x)
print("\n")

# Rudimentary way of doing the scoreboard for pool 1
mycursor.execute("SELECT pm.UserId, Count(*) FROM PoolMemberPrediction pmp INNER JOIN PoolMembership pm ON pmp.MembershipId = pm.MembershipId INNER JOIN Game g ON pmp.GameId = g.GameId WHERE PoolId = 1 AND g.HomeTeamWon = pmp.HomeTeamPred GROUP BY pm.UserId")
myresult = mycursor.fetchall()
for x in myresult:
    print(x)

# EXPECTED OUTPUT
#<mysql.connector.connection_cext.CMySQLConnection object at 0x000001880B702748>
#(1, 'Name1', 0)
#(2, 'Name2', 0)
#(3, 'Name3', 0)
#(4, 'Name4', 0)
#(5, 'Name5', 0)
#(6, 'Name6', 0)
#(7, 'Name7', 0)


#(1, 1, 'Atlanta Hawks', 'Atlanta', 'Some/URL')
#(2, 1, 'Boston Celtics', 'Boston', 'Some/URL')
#(3, 1, 'Brooklyn Nets', 'Brooklyn', 'Some/URL')
#(4, 1, 'Charlotte Hornets', 'Charlotte', 'Some/URL')
#(5, 1, 'Chicago Bulls', 'Chicago', 'Some/URL')
#(6, 1, 'Cleveland Cavaliers', 'Cleveland', 'Some/URL')
#(7, 1, 'Dallas Mavericks', 'Dallas', 'Some/URL')


#(1, 1, 3, datetime.date(2022, 1, 5), 1900, 1)
#(2, 4, 7, datetime.date(2022, 1, 5), 1930, 0)
#(3, 5, 2, datetime.date(2022, 1, 5), 2000, 0)


#(1, 1, 3, datetime.date(2022, 1, 5), 1900, 1, 'Atlanta Hawks', 'Brooklyn Nets')
#(2, 4, 7, datetime.date(2022, 1, 5), 1930, 0, 'Charlotte Hornets', 'Dallas Mavericks')
#(3, 5, 2, datetime.date(2022, 1, 5), 2000, 0, 'Chicago Bulls', 'Boston Celtics')


#(1, 1, 1, 1, None, 'Name1')
#(2, 1, 2, 0, None, 'Name2')
#(3, 1, 3, 0, None, 'Name3')


#(1, 3, 1, 0, 1)
#(1, 2, 1, 1, 1)
#(1, 1, 1, 0, 1)
#(1, 3, 2, 1, 0)
#(1, 2, 2, 1, 0)
#(1, 1, 2, 0, 0)
#(1, 3, 3, 0, 0)
#(1, 2, 3, 1, 0)
#(1, 1, 3, 0, 0)


#(2, 1)
#(1, 2)
#(3, 1)