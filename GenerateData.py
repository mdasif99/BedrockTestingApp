import csv
import random
import datetime
import sys
import string
import hashlib
import os
#
#Defining constants

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S.%f'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

START_DATE = '1900-01-01'
END_DATE = datetime.date.today().strftime('%Y-%m-%d')
START_DATETIME = '1900-01-01 00:00:00.000000'
END_DATETIME = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
START_TIME = '00:00:00.000000'
END_TIME = datetime.datetime.now().strftime('%H:%M:%S.%f')

dataTypeList = ['INTEGER', 'STRING', 'ALPHANUMERIC', 'DATE', 'TIME', 'DATETIME', 'FLOAT']
seqTypeList = ['UNIQUE', 'RANDOM', 'FIXED']

#Defining no of rows; will be added as a command line para later on
#numRows=100
f = open("property.config")
fl=f.readlines()
fileSize=int(fl[9].split('\t')[1])*1024*1024
f.close()
delim=','

#Array where config file will be stored
arrConfig=['']

#############################################
######FUNCTION TO LOAD CONFIG FILE###########
#############################################
def LoadConfig():
    #Opening the config file
    try:
        config=open ('Config.txt', 'r')
    except FileNotFoundError:
        print ("Please check if the config file exists...")
        sys.exit()

    #Populating the config file into an array
    row=1    #As the first line of the array is blank
    for line in config:
        #Strip line of newlines and spaces
        line=line.strip('\n')
        line=line.strip(' ')
        #Handle blank and commented lines
        if len(line)==0 or line[0] == '#':
            continue
        #Check if number of arguments is 7
        if row !=0 and line.count(',') != 6:
            print('Field ' + str(row) + ' of Config file does not have 7 arguments.')
            sys.exit()        
        #Append field details to array
        arrConfig.append (line.split(','))

        #Checking config file for errors
        if row !=0:            
            #Check if data types correctly provided
            if arrConfig[row][1].upper() not in dataTypeList:
                print ('Invalid data type in line ' + str(row))
                print ('Accepted types are: Integer, String, Alphanumeric, Date, Time, DateTime and Float.')
                sys.exit()
            #Check if size is an integer value
            if arrConfig[row][2].isdigit() == False:
                print ('Invalid size in line ' + str(row))
                print ('Expecting an integer value')
                sys.exit()           
            #Check if sequence is correctly provided
            if  arrConfig[row][3].upper() not in seqTypeList:
                print ('Invalid sequence type in line ' + str(row))
                print ('Accepted types are: Unique, Random and Float')
                sys.exit()
            #Unique implemented only for Integers
            if arrConfig[row][3].upper() == 'UNIQUE' and arrConfig[row][1].upper() != 'INTEGER':
                print ('Invalid values in line ' + str(row))
                print ('Unique applicable only for Integer data type')
                sys.exit()
            #Check type of Start and End for Integers
            if arrConfig[row][1].upper() == 'INTEGER' and arrConfig[row][3].upper() == 'RANDOM':
                if len(arrConfig[row][5]) != 0 and len(arrConfig[row][6]) != 0:
                    if arrConfig[row][5].isdigit() == False or arrConfig[row][6].isdigit() == False:
                        print ('Invalid value in line ' + str(row))
                        print ('Integers expected in Start and End')
                        sys.exit()
        
        row+=1
    #print (arrConfig)
    print ('Reading config file...Done')    
#############################################
################END FUNCTION#################
#############################################

#############################################
########FUNCTION TO GET RANDOM VALUES########
#############################################
def GetRandom (colType, colSize, colFormat, colStart, colEnd):
    
    tempDt = datetime.datetime.now()
    tempStr = ''
    tempInt = 0
    tempFloat = 1.1
    tempArr=[]

    startDtOrd = 0
    endDtOrd = 0

    #Getting ordinal values of date ranges and seconds for time ranges

    if colType == 'INTEGER':
        if len(colStart) != 0 and len(colEnd) != 0:
            tempInt = random.randrange (int(colStart), int(colEnd))
        else:
            tempInt = random.randrange (10**(colSize-1), ((10**colSize)-1))
        if colFormat != '':
            tempStr = colFormat % tempInt
        else:
            tempStr = tempInt
            
    elif colType == 'STRING':
        if len(colStart) != 0 and len(colEnd) != 0:
            tempStr = random.choice ([colStart, colEnd])
        else:
            tempStr = ''.join(random.choice(string.ascii_uppercase+string.ascii_lowercase) \
                                      for i in range(colSize))

    elif colType == 'ALPHANUMERIC':
        tempStr = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase \
                                                + string.digits) for x in range(colSize))

    elif colType == 'DATE':        
        #If range not specified
        if len(colStart) == 0 and len(colEnd) ==0:
            colStart = START_DATE
            colEnd = END_DATE
        #Get ordinal values
        startDtOrd = datetime.datetime.strptime (colStart, DATE_FORMAT).toordinal()
        endDtOrd = datetime.datetime.strptime (colEnd, DATE_FORMAT).toordinal()
        
        #Generating a random ordinal integer and converting it back to date
        tempDt = datetime.datetime.fromordinal(random.randint(startDtOrd, endDtOrd))

        if colFormat != '':
            tempStr = tempDt.strftime (colFormat)
        else:
            tempStr = tempDt
            
    elif colType == 'TIME':
        #If range not specified
        if len(colStart) == 0 and len(colEnd) ==0:
            colStart = START_TIME
            colEnd = END_TIME
        #Getting ordinal values for time i.e. converting to seconds
        colStart = datetime.datetime.strptime (colStart, TIME_FORMAT)
        colEnd = datetime.datetime.strptime (colEnd, TIME_FORMAT)
        startDtOrd = datetime.timedelta(hours = colStart.hour, minutes = colStart.minute, \
                    seconds = colStart.second , microseconds = colStart.microsecond).total_seconds()
        endDtOrd = datetime.timedelta(hours = colEnd.hour, minutes = colEnd.minute, \
                    seconds = colEnd.second , microseconds = colEnd.microsecond).total_seconds()
        
        secs = random.uniform (startDtOrd, endDtOrd)
        tdTime = str(datetime.timedelta (seconds = secs))
        
        tempDt = datetime.datetime.strptime (tdTime, TIME_FORMAT)
        if colFormat != '':
            tempStr = tempDt.strftime (colFormat)
        else:
            tempStr = tempDt

    elif colType == 'DATETIME':
        
        if len(colStart) == 0 and len(colEnd) ==0:
            colStart = START_DATETIME
            colEnd = END_DATETIME
        
        dt1 = colStart.split(" ")
        dt2 = colEnd.split(" ")
        fm = colFormat.split(" ")
        dt = GetRandom('DATE', 0, fm[0], dt1[0], dt2[0])
        tm = GetRandom('TIME', 0, fm[1], dt1[1], dt2[1])
        
        tempDt = datetime.datetime.strptime (dt + " " + tm, colFormat)
        if colFormat != '':
            tempStr = tempDt.strftime (colFormat)
        else:
            tempStr = tempDt
            
    elif colType == 'FLOAT':
        if len(colStart) != 0 and len(colEnd) != 0:
            tempFloat = random.uniform (float(colStart), float(colEnd))
        else:
            tempFloat = random.uniform (10**(colSize-1), ((10**colSize)-1))
        
        if colFormat != '':
            tempStr = colFormat % tempFloat
        else:
            tempStr = tempFloat

    return tempStr
##############################################
################END FUNCTION##################
##############################################

##############################################
######FUNCTION TO GET UNIQUE VALUES###########
##############################################

def GetUnique (colType, colSize, colFormat, numRow, numCol, arrData):
    uniqNum = 0

    if numRow == 0:
        uniqNum = 10**(colSize-1)
    else:
        uniqNum = int (arrData[numRow][numCol]) + 1
                
    if colFormat != '':
        tempStr = colFormat % uniqNum
    else:
        tempStr = uniqNum

    return tempStr

##############################################
################END FUNCTION##################
##############################################

##############################################
###############MAIN PROGRAM###################
##############################################

#Getting the number of rows as a command line argument
##if len(sys.argv) != 2:
##    print ('Please specify the number of rows to generate')
##    print ('\n')
##    sys.exit()
##numRows = int(sys.argv[1])

#Loading the Config file
LoadConfig()

#Opening the csv file for writing
with open ('csvOut.csv', 'w', newline  ='') as csvfile:
    outfile = csv.writer (csvfile, delimiter=',', quotechar='"', \
                          quoting=csv.QUOTE_NONE, dialect=csv.excel)
    print ('Writing csv file...')

    #Generated data to be stored here temporarily mostly for generating unique values
    arrData=['']        

    #Writing the csv file by iterating to the number of rows required
    #for row in range (0, numRows):
    row=0
    while csvfile.tell()<fileSize:
        tempRow = []
        strCol = ''
        colSize = 0
        colType = ''
        colSeq = ''
        colFormat = ''
        colStart = ''
        colEnd = ''

        #Iterating through the columns
        for col in range (0, len(arrConfig)):

            #Just in case there is a blank row
            if len (arrConfig[col])==0:       
                continue

            #Reading field details
            colType = str(arrConfig [col][1]).upper()
            colSize = int(arrConfig[col][2])
            colSeq = str(arrConfig [col][3]).upper()
            colFormat = str(arrConfig [col][4])
            colStart = str(arrConfig [col][5])
            colEnd = str(arrConfig [col][6])
                            
            if colSeq == 'RANDOM':
                strCol = GetRandom (colType, colSize, colFormat, colStart, colEnd)
            elif colSeq == 'FIXED':
                strCol = colStart
            elif colSeq == 'UNIQUE':
                strCol = GetUnique (colType, colSize, colFormat, row, col-1, arrData)

            #Adding the column value to the temp array
            tempRow.append (strCol)

        #Writing the row to a temp array
        arrData.append (tempRow) 

        #Finally writing the populated row to the csv file
        outfile.writerow (tempRow)
        row+=1
    print ('Writing csv file...Done')
#############################################################
