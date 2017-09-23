import json
import requests
import sys
##############################################################		LOGIN
serverBedrock='192.168.2.150:9090'
uLogin='http://'+serverBedrock+'/bedrock-app/services/rest/login'
credentials={"username":"admin","password":"admin"}
try:
	r1=requests.post(uLogin,data=json.dumps(credentials))
	z1=r1.json()
except:
	print 'error with login request'
	print '\npress enter to exit'
	raw_input()
	sys.exit()
if len(z1)==2:
	print 'error logging in to',server
	print '\npress enter to exit'
	raw_input()
	sys.exit()
print z1['responseMessage']
############################################################### 	LZ Server
uFetchServer='http://'+serverBedrock+'/bedrock-app/services/rest/ingestion/publish/retrieveLandingZoneServers'
try:
	r2=requests.get(uFetchServer,cookies=r1.cookies)
	z2=r2.json()
except:
	print 'error with fetch server request'
	print '\npress enter to exit'
	raw_input()
	sys.exit()
try:
	exceptionTest=z2['status']['responseCode']
except:
	print 'error with params'
	print '\npress enter to exit'
	raw_input()
	sys.exit()
#		Find required server_Id
server_id='0'
if z2['status']['responseCode']!=204:						
	for server in z2['result']['data']:
		if server['ipAddress']=='192.168.2.150':
			server_id=server['serverId']
if server_id=='0':
	#	CREATE SERVER
	uCreateServer='http://'+serverBedrock+'/bedrock-app/services/rest/ingestion/publish/saveLZServer'
	credentials={
     "serverName": "intern",
     "ipAddress": "192.168.2.150",
     "username": "hudson",
     "password": "zaloni.1234"}
	try:
		r2c=requests.post(uCreateServer,json=credentials,cookies=r1.cookies)
		z2c=r2c.json()
		#print z2c
	except:
		print 'error with create server request'
		print '\npress enter to exit'
		raw_input()
		sys.exit()
	if z2c['status']['responseCode']==204:
		print 'server not created'
		print '\npress enter to exit'
		raw_input()
		sys.exit()
	print 'created new lz server'
	server_id=str(z2c['status']['result'])
print 'LZ server id:',server_id
############################################################# LZ Directory
uFetchDirectory='http://'+serverBedrock+'/bedrock-app/services/rest/ingestion/publish/getLzDirectoriesByServer/'+server_id
try:
	r3=requests.get(uFetchDirectory,cookies=r1.cookies)
	z3=r3.json()
except:
	print 'error with fetch directory request'
	print '\npress enter to exit'
	raw_input()
	sys.exit()
try:
	exceptionTest=z3['status']['responseCode']
except:
	print 'error with params'
	print '\npress enter to exit'
	raw_input()
	sys.exit()
#	Search for directory
dir_id=0
if z3['status']['responseCode']==200:
	for directory in z3['result']['data']:
		if directory['dirPath']=='/home/hudson/intern_2017/version3/data':
			dir_id=directory['lzDirId']
if dir_id==0:
	#	Create directory
	uCreateDirectory='http://'+serverBedrock+'/bedrock-app/services/rest/ingestion/publish/saveLZDirectories'
	credentials= {
	 "fileSystemId": 1,
	 "totalSlaves": "1",
	 "serverId": int(server_id),
	 "dirPath": "/home/hudson/intern_2017/version3/data",
	 "description": "",
	 "fileSystemUri": "",
	 "notificationDetails": [],
	 "fileSystemConnectionInstanceId": 1,
	 "allowIngestion": True,
	 "lzDirId": 0}
	#credentials=json.dumps(credentials)
	#print credentials
	try:
		r3c=requests.post(uCreateDirectory,json=credentials,cookies=r1.cookies)
		z3c=r3c.json()
		#print z3c
	except:
		print 'error with create directory request'
		print '\npress enter to exit'
		raw_input()
		sys.exit()
	if z3c['status']['responseCode']==412:
		print 'server not found'
		print '\npress enter to exit'
		raw_input()
		sys.exit()
	print 'created new lz directory'
	dir_id=z3c['status']['result']
print 'LZ directory id:',dir_id
