import time
import os.path
import random
from tempfile import mkstemp
from shutil import move
from os import remove, close
from one import *
from subprocess import call
#from subprocess import check_output
from subprocess import PIPE,Popen
def replace(file_path, pattern, subst):
    #Create temp file
    fh, abs_path = mkstemp()
    with open(abs_path,'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(line.replace(pattern, subst))
    close(fh)
    #Remove original file
    remove(file_path)
    #Move new file
    move(abs_path, file_path)
# take iput from property.config file
f = open("property.config")
fl=f.readlines()
noFilePattern=int(fl[1].split('\t')[1])
noFile=int(fl[3].split('\t')[1])
filePerDrop=int(fl[5].split('\t')[1])
#timeInterval=fl[7].split('\t')[1]
minTime=int(fl[7].split('\t')[1])
maxTime=int(fl[7].split('\t')[3])
fileSize=int(fl[9].split('\t')[1])*1024*1024
metaExtension=fl[11].split('\t')[1].split("\"")[1]
controlExtension=fl[13].split('\t')[1].split("\"")[1]
triggerExtension=fl[15].split('\t')[1].split("\"")[1]
#print metaExtension
f.close()
#	Create File patterns
#print 'done reading',noFilePattern,noFile,filePerDrop,timeInterval,fileSize
regex=[]
for i in range(noFilePattern):
	fpPrefix=str(time.time())+'_[0-9]+_'
	fpSuffix='.csv'
	regex.append(fpPrefix+fpSuffix)
	uCreatePattern='http://'+serverBedrock+'/bedrock-app/services/rest/projects/1/filePatterns'

	credentials = {
    "patternPrefix": fpPrefix,
    "patternSuffix": fpSuffix,
    "otherFiles": "",
    "description": "",
    "destination": "/user/hudson/intern_2017",
    "destinationFileSystemUri": "hdfs://hdpqa-n1.zalonilab.com:8020",
    "retryAttempts": "0",
    "triggerFileSuffix": triggerExtension,
    "deleteAfterIngestion": "true",
    "globalParameter": "",
    "lzDirectories": [ {
                "lzDirId": dir_id
        }
       
    ],
    "frequency": "1",
    "frequencyUnit": "",
    "wfLevelParamList": "",
    "delimiter": "",
    "entityId": "",
    "entityVersion": "",
    "destinationFileSystemProperties": {
         "": ""
     },
    "workflowId": "",
    "dataFileFormatId": "",
    "dataFormat": "",
    "filePatternId": 0,
    "adminCapacityQueues": {},
    "controlFileSuffix": controlExtension,
    "metaFileSuffix": metaExtension,
    "fileSystemConnectionInstanceId": "2",
    "fileSystemId": "1",
    "ingestAs" :"hudson",
    "checksumFileSuffix" :""
 	}
 	try:
		#print 'in try'
		r3c1=requests.post(uCreatePattern,json=credentials,cookies=r1.cookies)
		z3c1=r3c1.json()
		#print z3c1
		print 'created pattern:',fpPrefix+fpSuffix
	except:
		print 'error with create pattern request'
		print '\npress enter to exit'
		raw_input()
		sys.exit()
	time.sleep(0.01)
#	Create file to injest
if os.path.isfile('ingestSample.csv'):
	call(['rm','ingestSample.csv'])
if metaExtension=="":
	fl = open("ingestionMetaDefault.meta",'r')
	'''f=open('ingestSample.csv','w')				#create propert file and generate data, let it be 'ingestSampe.csv'
	f.write('1\tname\t600.5\n')
	while f.tell()<fileSize: 
		f.write('1\tname\t600.5\n')
	f.close()'''
else:
	fl = open("ingestionMeta"+metaExtension,'r')
call(['cp','Config.txt.template','Config.txt'])
fl1 = open("Config.txt", 'a')
for line in fl.readlines():
    if line.strip() == "Source_Name|Schema_Name|TABLE_NAME|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|DATA_SCALE|Format|Primary_Keys|COLUMN_ID|SENSITIVITY":
        continue
    else:
        params = line.strip().split('|')
        fl1.write(params[3]+',')
        if params[4] == 'INT':
            fl1.write('INTEGER,')
        elif params[4] == 'STRING':
            fl1.write('STRING,')
        elif params[4] == 'DATE':
            fl1.write('DATE,')
        elif params[4] == 'TIME':
            fl1.write('TIME,')
        elif params[4] == 'FLOAT':
            fl1.write('FLOAT,')
        if params[5] != '':
            fl1.write(params[5]+',')
        else:
            fl1.write('0,')
        if params[9] == '1':
            fl1.write('Unique,')
        else:
            fl1.write('Random,')
        fl1.write(',,')
        fl1.write('\n')
fl.close()
fl1.close()
call(['python3','GenerateData.py'])
call(['mv','csvOut.csv','ingestSample.csv'])
fl = open("ingestionMeta"+metaExtension,'r')
line=fl.readlines()[1]
sourcePlatform=line.split('|')[0]#read from metadata file
schemaName=line.split('|')[1]#read from metadata file
fl.close()
#												creating the control file
if controlExtension!="":
	call(['cp','DH_Sample_01.ctl','controlSample'+controlExtension])
	pt="sourceplatform: MEDIA";
	st="sourceplatform: "+sourcePlatform;
	replace('controlSample'+controlExtension,pt,st)
	pt="schemaname: HSR";
	st="schemaname: "+schemaName;
	replace('controlSample'+controlExtension,pt,st)
#												creating the trigger file
if triggerExtension!="":
	if os.path.isfile('triggerSample'+triggerExtension):
		call(['rm','triggerSample'+triggerExtension])
	f=open('triggerSample'+triggerExtension,'w')
	f.write('')
	f.close()
#	Creating files
#filenames = []
if noFilePattern>noFile:
	noFilePattern=noFile
filePerPattern=noFile/noFilePattern
remainingFiles=noFile%noFilePattern
c=0
if os.path.isfile('intendedIngestions.txt'):
	call(['rm','intendedIngestions.txt'])
for i in range(noFilePattern):
	for j in range(filePerPattern):
		newNamePrefix=regex[i].split('_')[0]+'_'+str(j)+'_'
		newName=regex[i].split('_')[0]+'_'+str(j)+'_.csv'
		#filenames.append(newName)
		f=open('intendedIngestions.txt','a')
		f.write(newName+'\n')
		f.close()
		if c==filePerDrop:
			c=0
			if(minTime==maxTime):
				time.sleep(minTime)
			else:
				time.sleep(random.randint(minTime,maxTime+1))
		else:
			c+=1
		#data file
		call(['cp','ingestSample.csv','data/'+newName])
		print 'copied: data',newName,
		#meta file
		if metaExtension!="":
			call(['cp',"ingestionMeta"+metaExtension,'data/'+newNamePrefix+metaExtension])
			print 'copied: metadata',newNamePrefix+metaExtension,
		#control file
		if controlExtension!="":
			#checksum=check_output(['cksum','data/'+newName]).split(' ')[0]
			proc=Popen(['cksum','one.py'],stdout=PIPE)
			checksum=proc.communicate()[0].split()[0]
			call(['cp','controlSample'+controlExtension,newNamePrefix+controlExtension])
			pt="checksum:d131dd02c5e6eec4";
			st="checksum:"+checksum;
			replace(newNamePrefix+controlExtension,pt,st)
			call(['mv',newNamePrefix+controlExtension,'data/'+newNamePrefix+controlExtension])
			print 'copied control:',newNamePrefix+controlExtension,
		#trigger file
		if triggerExtension!="":
			call(['cp','triggerSample'+triggerExtension,'data/'+newNamePrefix+triggerExtension])
			print 'copied trigger:',newNamePrefix+triggerExtension,
		print '\nCopied set:',newNamePrefix
		#copy singestSampple.csv to regex[i] as newName
for x in regex:
	if remainingFiles==0:
		break
	remainingFiles-=1
	newNamePrefix=x.split('_')[0]+'_'+str(filePerPattern)+'_'
	newName=x.split('_')[0]+'_'+str(filePerPattern)+'_.csv'
	#filenames.append(newName)
	f=open('intendedIngestions.txt','a')
	f.write(newName+'\n')
	f.close()
	if c==filePerDrop:
		c=0
		if(minTime==maxTime):
			time.sleep(minTime)
		else:
			time.sleep(random.randint(minTime,maxTime+1))
	else:
		c+=1
	#data file
	call(['cp','ingestSample.csv','data/'+newName])
	print 'copied: data',newName,
	#meta file
	if metaExtension!="":
		call(['cp',"ingestionMeta"+metaExtension,'data/'+newNamePrefix+metaExtension])
		print 'copied: metadata',newNamePrefix+metaExtension,
	#control file
	if controlExtension!="":
		#checksum=check_output(['cksum','data/'+newName]).split(' ')[0]
		proc=Popen(['cksum','one.py'],stdout=PIPE)
		checksum=proc.communicate()[0].split()[0]
		call(['cp','controlSample'+controlExtension,newNamePrefix+controlExtension])
		pt="checksum:d131dd02c5e6eec4";
		st="checksum:"+checksum;
		replace(newNamePrefix+controlExtension,pt,st)
		call(['mv',newNamePrefix+controlExtension,'data/'+newNamePrefix+controlExtension])
		print 'copied control:',newNamePrefix+controlExtension,
	#trigger file
	if triggerExtension!="":
		call(['cp','triggerSample'+triggerExtension,'data/'+newNamePrefix+triggerExtension])
		print 'copied trigger:',newNamePrefix+triggerExtension,
	print '\nCopied set:',newNamePrefix
	#copy copy
	
