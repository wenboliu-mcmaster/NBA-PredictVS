# configureCWD.py - Sets current working directory relative to where program folder is located
# https://github.com/JakeKandell/NBA-Predict/tree/master/Data by Jake Kandell
import os

# Sets current working directory relative to where program folder is located
def setCurrentWorkingDirectory(directoryName):

    programDirectory = os.path.dirname(os.path.abspath(__file__))
    newCurrentWorkingDirectory = os.path.join(programDirectory, directoryName)
    os.chdir(newCurrentWorkingDirectory)