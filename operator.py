from __future__ import print_function
from is_wire.core import Channel, Message, Subscription, Logger, Status
import socket
from is_msgs.common_pb2 import Position
from RequisicaoRobo_pb2 import RequisicaoRobo
from random import randint
import time

log = Logger(name='Operator')
request_type = RequisicaoRobo()

# connect to the broker and create a subscription
log.info('Creating channels...')
channel = Channel("amqp://guest:guest@localhost:5672")
subscription = Subscription(channel)
subscription.subscribe(topic="Controle.Operator")

# Funct to send messages from operator to console
def sendMsg(content, topic):

    try:
        request = Message(content=content.encode('latin1'))
    except:
        request = Message(content=content)

    channel.publish(request, topic=topic)

log.info('Creating TURN ON message')
log.info('Sending TURN ON message...')
sendMsg('Try request', 'Controle.Console')

sys_online = False

#main loop
while True:

        log.info('Waiting for reply...')
        reply = channel.consume()

        time.sleep(2)


        if reply.body.decode('latin1') == 'System is online!':

            log.info('System is online!')
            sys_online = True
            break

        else:

            log.info('System is offline. Trying again...')
            log.info('Sending TURN ON message...')
            time.sleep(2)
            sendMsg('TURN ON', 'Controle.Console')

def requestPosition(type):

   time.sleep(2)

   subscription1 = Subscription(channel)

   if type == 'get':

     request_type.function = 'GET POSITION'

   elif type == 'set':

     request_type.function = 'SET POSITION'
     request_type.positions.x = randint(1, 50)
     request_type.positions.y = randint(1, 50)

   else:

     log.info('Creating FALSE FUNCTION request')
     log.info('Sending FALSE FUNCTION request...')
     request_type.function = 'FALSE FUNCTION'

   if type == 'get' or type == 'set':

     log.info('Getting a randomized ID')
     request_type.id = randint(1,5)

     log.info(f'ROBOT ID: {request_type.id}')

     log.info(f'Creating {request_type.function} request...')
     log.info(f'Sending {request_type.function} request...')

   request = Message(content=request_type, reply_to=subscription)
   channel.publish(request, topic="Controle.requests")

   log.info(f'Waiting {request_type.function} reply...')
 
   try:

     reply = channel.consume()
#    print(reply)
     requisicaoRobo = RequisicaoRobo()
     requisicaoRobo = reply.unpack(RequisicaoRobo)
     if type == 'get':
       log.info(f'ROBOT ID: {requisicaoRobo.id} - FUNCTION: {requisicaoRobo.function} X: {requisicaoRobo.positions.x} - Y: {requisicaoRobo.positions.y}')
     elif type == 'set':

       log.info(f'SET POSITION Reply: {reply.status.code}')
     else:
       print(reply)
       log.info(f'FALSE FUNCTION Reply: {reply.status.code}')

   except:
     log.error('No reply :(')

requestPosition('get')
requestPosition('get')
requestPosition('set')
requestPosition('set')
requestPosition('get')
requestPosition('get')
requestPosition('false')

