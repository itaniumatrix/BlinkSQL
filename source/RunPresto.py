from datetime import datetime
from queryMap import *
import subprocess
import time

#install PyHive 0.1.6 to connect DB-API 
from pyhive import presto

def timeOp(dataframeOp):
    t0 = time.time()
    output = eval(dataframeOp)
    t1 = time.time()
    print(output)
    print('*Query time: ' + str((t1 - t0))+'*')

##Lazy Eval (need to time .fetchAll() call, like .collect() in Spark)
"""
cursor = presto.connect('localhost').cursor()
timeOp("cursor.execute('SELECT count(1) from f09')")
#%time print cursor.fetchone()
get_ipython().magic(u'time print cursor.fetchall()')
"""

"""Translate UTC time to readable %Y-%m-%d"""
def getTimeString(utcTime):
    return datetime.fromtimestamp(int(utcTime)).strftime('%Y-%m-%d,%H:%M:%S')

"""List of SQL queries - q1 - 4 is core. 5,6 variations on 4 by selecting fewer columns"""
##mapQuery, dictionary imported from queriesMap file

# Return the first query result and start time, and time taken
def runTimeQuery(year, queryString):
    tableNameHive = "f" + str(year)[2:]
    queryString = mapQuery[queryID].replace("SparkTempTable", tableNameHive)
    cursor.execute(queryString)
    t0 = time.time()
    result = cursor.fetchall()
    delta_t = time.time() - t0
    if (len(result) == 0):
        return ("Null", t0, delta_t)
    else:
        return (str(result[0]), t0, delta_t)

#Write to file
#with open("test.txt", "a") as myfile:
#        myfile.write(getTimeString(t0) + "," + str(query) + "," + str(result[0]) + "\n")

##Result is a 2-tuple, with time taken and TECHNOLOGY+NUM WORKER NODES (Presto6)
def appendResultToFile(filename, startTime, queryID, year, timeTaken, result):
    outString = "{},{},{},{:.2f},{},{}\n".format(getTimeString(time.time()), queryID, year, timeTaken, result, "Presto6")
    with open(filename, "a") as myfile:
        myfile.write(outString)
    print(outString)


cursor = presto.connect('localhost').cursor()
for year in [2007,2009,2012]:
    for queryID in range(1,5):
        #Repeat 2 times
        for i in range(1,4):
            result, t0, time_taken = runTimeQuery(year, mapQuery[queryID])
            appendResultToFile("PrestoQueries.txt", t0, queryID, year, time_taken, result)



