INGESTION TESTING TOOL
======================

DESCRIPTION
----------- 

A tool which can create take in various parameters for an ingestion process and after the ingestion process is completed, reports the ingestion statistics in a .xlsx file .

PREREQUISITE
------------

1. Python2.6 or higher with the following packages:
	1. json  
	2. requests  
	3. sys  
	4. math  
	5. os  
	6. time  
	7. random  
	8. subprocess  
	9. tempfile  
	10. shutil  
2. Python3.5 or higher with the following packages:
	1. csv  
	2. random  
	3. datetime  
	4. sys  
	5. string  
	6. hashlib  
	7. os  

Steps
-----

1. Edit 'prpoerty.config' file to fill in necessary parameters. A template for the same is provided as well ('property.config.template')
2. Configure a meta file as according to the schema needed with prefix 'ingestionMeta' and suffix of what you hav entered in the 'property.config' file
3. Use command 'python three.py' inside folder 'version1' to run start the program
4. Report will be generated in the file 'ingestionReport.xlsx'

Other notes
-----------

1. All credentials used are based on bedrock at 192.168.2.150
2. LZ server is made to be at 192.168.2.150 authorized by user 'hudson@192.168.2.150'
3. LZ directory is made to be at 192.168.2.150 authorized by user 'hudson@192.168.2.150' at 'hudson@192.168.2.150:/home/hudson/intern_2017/version<X>/data'
4. The destination is at cluster:
	192.168.2.147 (name node)  
	192.168.2.148   
	192.168.2.149  
	192.168.2.150 (edge node used)  
	192.168.2.164   
	192.168.1.194   
5. Destination file system URI: 'hdfs://hdpqa-n1.zalonilab.com:8020'
6. Destination folder at hdfs: '/user/hudson/intern_2017'
7. The configurations done are for soruce files placed at 'hudson@192.168.2.150:/home/hudson/intern_2017/version<X>/'

Authors
--------

1. Nitish Kashyap
2. Md. Asif
-Summer Interns (may-july, 2017) at Zaloni Technologies India Pvt. Ltd.
