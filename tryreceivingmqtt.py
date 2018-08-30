#pip install PyMySQL
import pymysql.cursors #import database lib
#pip install paho-mqtt
import paho.mqtt.client as mqttClient
import datetime #import date time system
now = datetime.datetime.now() #get current time


import json
import time

#Connect to Database
connection = pymysql.connect(host = 'localhost', #host of database
user = 'root', #user of database
password = '', #password of database
db = 'HeinekenRef', #database you want to use
charset = 'utf8mb4',
cursorclass = pymysql.cursors.DictCursor)
print ('connect network successfull')

#Connect mqtt function  
def on_connect(client, userdata, flags, rc):
 
    if rc == 0:
 
        print("Connected to broker")
 
        global Connected                #Use global variable
        Connected = True                #Signal connection 
 
    else:
 
        print("Connection failed")

#Message receive mqtt function 
MessArrived = None
dateReceive = None
timeReceive = None
def on_message(client, userdata, message):
    MessArrived = message.payload
    print "Message received: "  + MessArrived
    #Process json
    j = json.loads(MessArrived)
    idTu = j['id']
    dateReceive = str(now.day) + '/' + str(now.month) + '/' + str(now.year)
    timeReceive = str(now.hour) + ':' + str(now.minute) + ':' + str(now.second)
    minReceive = j['min']
    maxReceive = j['max']
    Temp1 = j['nhietdo1']
    Temp2 = j['nhietdo2']
    Status1 = j['may1']
    Status2 = j['may2']
    #Insert to database
    with connection.cursor() as cursor:
	sqlCompare = 'SELECT minReceive,maxReceive,Temp1,Temp2 FROM `Record_Table` ORDER BY ID DESC LIMIT 1'
	cursor.execute(sqlCompare)
	resultToCompare = cursor.fetchone()
	if (resultToCompare != None):
		lastmin = resultToCompare['minReceive']
		lastmax = resultToCompare['maxReceive']
		lastTemp1 = resultToCompare['Temp1']
		lastTemp2 = resultToCompare['Temp2']
		if(lastmin == minReceive ) and (lastMax == maxReceive) and (lastTemp1 == Temp1) and (lastTemp2 == Temp2):
			print ('No insert to database')
		else:
			sqlNextResult = "INSERT INTO Record_Table(id,dateReceive,timeReceive,minReceive,maxReceive,Temp1,Temp2,Status1,Status2) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
			cursor.execute(sqlNextResult ,(idTu,dateReceive,timeReceive,minReceive,maxReceive,Temp1,Temp2,Status1,Status2))
			connection.commit()
			print('Insert to database sucessful')
	else:
		sqlFirst = "INSERT INTO Record_Table(id,dateReceive,timeReceive,minReceive,maxReceive,Temp1,Temp2,Status1,Status2) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
		cursor.execute(sqlFirst,(idTu,dateReceive,timeReceive,minReceive,maxReceive,Temp1,Temp2,Status1,Status2))
		connection.commit()
		print 'Insert first result to database successful'
    
 
Connected = False   #global variable for the state of the connection

#MQTT information
topic = str(raw_input("Please enter mqtt topic: ")) 
broker_address= str(raw_input("Please enter host mqtt: "))   #Broker address
port = raw_input("Please enter port mqtt: ")                        #Broker port
user = str(raw_input("Please enter mqtt username: "))
passwd = str(raw_input("Please enter mqtt password: "))
client = mqttClient.Client("Control1") #create new instance
if(user != None ) and (passwd != None):
	client.username_pw_set(user, passwd) #(user,password) your mqtt server
else:
	print('You skip enter username or password')

#client.username_pw_set(user, password=password)    #set username and password
client.on_connect= on_connect                      #attach function to callback
client.on_message= on_message                      #attach function to callback
 
client.connect(broker_address, port=port)          #connect to broker
 
client.loop_start()        #start the loop

#Start receive from mqtt
 
while Connected != True:    #Wait for connection
    time.sleep(0.1)
 
client.subscribe(topic)
 
try:
    while True:
        time.sleep(1)
 
except KeyboardInterrupt:
    print "exiting"
    client.disconnect()
    client.loop_stop()
