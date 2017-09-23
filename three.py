import math
import os
from two import *
waitTime=10*(noFile/filePerDrop+1)
print 'waiting for',waitTime,'seconds'
time.sleep(waitTime)
print 'waiting till end of ingestion'
while len(next(os.walk('data'))[2])>0:
	time.sleep(5)
filenames=[]
f=open('intendedIngestions.txt','r')
fl=f.readlines()
for files in fl:
	filenames.append(files.strip('\n'))
f.close()
#print 'array of files:',filenames
uIngestionHistory='http://'+serverBedrock+'/bedrock-app/services/rest/ingestion/ingestions/search' 
credentials= {
   "ingestionProperties": {
   "fileName":filenames
   },
   "pageProperties": {
	"chunkSize":2*len(filenames)
	}
 }
#print filenames
try:	
	res=requests.post(uIngestionHistory,json=credentials,cookies=r1.cookies)
	if str(res)=='<Response [204]>':
		print 'Empty history for current search'
		print '\npress enter to exit'
		raw_input()
		sys.exit()
	response=res.json()
	#print response
except:
	print 'error with ingestion history request'
	print '\npress enter to exit'
	raw_input()
	sys.exit()
failed=[]
times=[]
no_suc = 0
for info in response['result']:
	if info["status"]=='SUCCESS':
		times.append([info["id"],str(int(info["ingestionTime"])-int(info["startTime"])),info["sourceSize"],info["targetSize"]])
		no_suc += 1
	else:
		failed.append([info["id"],info["sourceSize"],info["targetSize"],info["error"]])
file=open('ingestionReport.xlsx','w')
file.write("Total files initialed to be ingested:\t"+str(len(filenames))+"\n")
print "Total files initialed to be ingested:\t"+str(len(filenames))+"\n"
file.write("Total file patterns created:\t"+str(noFilePattern)+"\n")
print "Total files file patterns created:\t"+str(noFilePattern)+"\n"
file.write("Size of each file:\t"+str(fileSize)+" bytes\n")
print "Size of each file:\t"+str(fileSize)+" bytes\n"
file.write("\n")
total_time = 0
for x in times:
	total_time += int(x[1])
#print total_time
#print float(total_time)/float(no_suc)
total_time=float(total_time)/1000.0
day=0
hour=0
minutes=0
time=float(total_time)
seconds=int(total_time)
miliseconds=int(math.ceil((10**(len(str(time))-len(str(seconds))-1))*(time-seconds)))
if seconds/60>0:
	minutes=seconds/60
	seconds=seconds-minutes*60
if minutes/60>0:
	hour=minutes/60
	minutes=minutes-hour*60
if hour/24>0:
	day=hour/24
	hour=hour-day*24
s=""
if day>0:
	s+=str(day)+"days "
if hour>0:
	s+=str(hour)+"hrs "
if minutes>0:
	s+=str(minutes)+"mins "
if seconds>0:
	s+=str(seconds)+"secs "
if miliseconds>0:
	milimili=str(miliseconds)
        if len(milimili)>3:
                milimili=milimili[:3]+'.'+milimili[3:]
        s+=milimili+"ms "
file.write("Successful ingestions:\n")
file.write("Total Ingestion Time per file:\t"+s+"\n")
print "Total Ingestion Time per file:\t"+s+"\n"
avg_time = total_time/float(no_suc)
s = ""
day=0
hour=0
minutes=0
time=float(avg_time)
seconds=int(avg_time)
miliseconds=int(math.ceil((10**(len(str(time))-len(str(seconds))-1))*(time-seconds)))
if seconds/60>0:
	minutes=seconds/60
	seconds=seconds-minutes*60
if minutes/60>0:
	hour=minutes/60
	minutes=minutes-hour*60
if hour/24>0:
	day=hour/24
	hour=hour-day*24
s=""
if day>0:
	s+=str(day)+"days "
if hour>0:
	s+=str(hour)+"hrs "
if minutes>0:
	s+=str(minutes)+"mins "
if seconds>0:
	s+=str(seconds)+"secs "
if miliseconds>0:
	milimili=str(miliseconds)
	if len(milimili)>3:
		milimili=milimili[:3]+'.'+milimili[3:]
	s+=milimili+"ms "
file.write("No. of succesful ingestions:\t"+str(len(times))+"\n")
print "No. of succesful ingestions:\t"+str(len(times))+"\n"
file.write("Average Ingestion Time per file:\t"+s+"\n")
print "Average Ingestion Time per file:\t"+s+"\n"
file.write('\n')
file.write('Failed Ingestions:')
file.write('\n')
if len(failed)==0:
	file.write('No results to display')
else:
	file.write("No. of failed ingestions:\t"+str(len(failed))+"\n")
	print "No. of failed ingestions:\t"+str(len(failed))+"\n"
	file.write('\n')
	file.write("Ingestion Id\tSource file size\tDestination file size\tError")
file.write('\n')
for x in failed:
	#print x
	for y in x:
		file.write(str(y)+'\t')
	file.write('\n')
file.close()
print 'Report has been generated on file \'ingestionReport.xlsx\''




	
