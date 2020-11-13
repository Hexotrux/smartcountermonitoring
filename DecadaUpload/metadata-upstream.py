import paho.mqtt.client as mqtt
import time
import decada_python_client
import os
import time
import json

Jmessage = ''

def on_connect(client, userdata, flags, rc): #flags is important if not on_connect won't be called
    print("Connection attempt returned: " + mqtt.connack_string(rc))
    print("Successfully connected to MQTT Broker @ localhost")

def on_message(client, userdata, message):
    global Jmessage
    topic = str(message.topic)
    #Jmessage = str(message.payload.decode("utf-8"))
    Jmessage = message.payload.decode("utf-8")
    print (Jmessage)
    with open("files/metadata.json", "a+") as f:
        f.write(Jmessage + "\n")


ourClient = mqtt.Client() #create MQTT client obj
ourClient.connect("localhost", 1883) #Connect to MQTT broker
ourClient.on_connect = on_connect
#ourClient.subscribe("bvcd/camera/event/enter_field", 2) #subscribe to topic
ourClient.subscribe("bvcd/camera/occupancy", 0)
ourClient.subscribe([("bvcd/camera/counter", 0), ("bvcd/camera/event/enter_field", 2), ("bvcd/camera/event/loitering", 2)])
ourClient.on_message = on_message # attach the "messagefunction" to the message event

ourClient.loop_start() #Start the MQTT client

decadaconn = decada_python_client.DecadaPythonClient(os.getcwd(), "/config.yaml") #Connect to DECADA
decadaconn.connect()

while True:
    #with open("files/metadata.json", "r") as f:
     #  message = f.readlines()
    #for Jmessage in message:
    if (Jmessage != ''):
        print("Json: ", Jmessage)
        Dmessage = json.loads(Jmessage) #change json to dict

        if 'eventType' in Dmessage:
            #Split data into it's respective model elements
            if Dmessage['eventType'] == 'loitering':
                AttrMessage = {
                    'ID': Dmessage['ID'],
                }

                MPMessage = {
                    'UTC': Dmessage['UTC'],
                    'ObjectID': Dmessage['ObjectID'],
                    'ObjectClass': Dmessage['ObjectClass'],
                    'ruleID': Dmessage['ruleID'],
                    'eventType': Dmessage['eventType'],
                    'eventTypeNo':2,
                    'Loitering':'Loitering detected!'
                 }

                EventMessage = {
                    'eventType': Dmessage['eventType']
                }

                print("Dict mode: ", Dmessage)
                print("Attributes: ", AttrMessage)
                print("Measure Points: ", MPMessage)
                print("Event: ", EventMessage)

                decadaconn.postMeasurePoints(MPMessage)  # Post measurepoints to DECADA
                decadaconn.updateAttributes(AttrMessage)  # Post attributes to DECADA
                decadaconn.postEvent(EventMessage)  # Post events to DECADA


            elif Dmessage['eventType'] == 'enter_field':
                AttrMessage = {
                    'ID':Dmessage['ID'],
                }

                MPMessage = {
                    'UTC':Dmessage['UTC'],
                    'ObjectID':Dmessage['ObjectID'],
                    'ObjectClass':Dmessage['ObjectClass'],
                    'ruleID': Dmessage['ruleID'],
                    'eventType':Dmessage['eventType'],
                    'eventTypeNo':1,
                    'Counter_Alert':'Counter assistance required!'
                 }

                EventMessage = {
                    'eventType':Dmessage['eventType']
                }

                print("Dict mode: ", Dmessage)
                print("Attributes: ", AttrMessage)
                print("Measure Points: ", MPMessage)
                print("Event: ", EventMessage)

                decadaconn.postMeasurePoints(MPMessage) # Post measurepoints to DECADA
                decadaconn.updateAttributes(AttrMessage) # Post attributes to DECADA
                decadaconn.postEvent(EventMessage) # Post events to DECADA

            Jmessage = ''
            time.sleep(5)

        else:
            counter_dict = Dmessage['Counters']
            #if counter_dict[0] != -1:
            if counter_dict[5] != -1:
                occupancy = counter_dict[5]
                #occupancy = counter_dict[0]
                AttrMessage = {
                    'ID':Dmessage['ID']
                }

                MPMessage = {
                    'UTC':Dmessage['UTC'],
                    'Occupancy':occupancy
                }

                print("Occupancy values")
                print("Dict mode: ", Dmessage)
                print("Attributes: ", AttrMessage)
                print("Measure Points: ", MPMessage)
                decadaconn.postMeasurePoints(MPMessage)
                decadaconn.updateAttributes(AttrMessage)

            else:
                leaving = counter_dict[1]
                entering = counter_dict[2]

                AttrMessage = {
                    'ID':Dmessage['ID']
                }

                MPMessage = {
                    'Entry':entering,
                    'Exits':leaving
                }

                print("Counter entry/exit")
                print("Dict mode: ", Dmessage)
                print("Attributes: ", AttrMessage)
                print("Measure Points: ", MPMessage)

                decadaconn.postMeasurePoints(MPMessage)
                decadaconn.updateAttributes(AttrMessage)

        Jmessage = ''
        time.sleep(5)

    else:
        time.sleep(1)