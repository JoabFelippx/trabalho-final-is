from __future__ import print_function
from is_wire.rpc import ServiceProvider, LogInterceptor
from is_wire.core import Channel, Message, Subscription, Logger, Status, StatusCode
from google.protobuf.empty_pb2 import Empty
import socket
from is_msgs.common_pb2 import Position
from RequisicaoRobo_pb2 import RequisicaoRobo
import time


log = Logger(name='Console')
empty = Empty()
log.info('Creating channels...')

channel = Channel("amqp://guest:guest@localhost:5672")
subscription = Subscription(channel)
subscription.subscribe(topic="Controle.Console")

log.info('Waiting TURN ON message...')

def sendMsg(content, topic):

    try:
        request = Message(content=content.encode('latin1'))
    except:
        request = Message(content=content)

    channel.publish(request, topic=topic)

while True:
    print('shadsga')    
    reply = channel.consume()
    print(reply)
    log.info('Message received. Checking content and trying to bring the system online...')

    time.sleep(1)
    if reply.body.decode('latin1') == 'TURN ON':

        log.info('System is online!')
        log.info('Sending notification to the operator...')

        sendMsg('System is online!', 'Controle.Operator')

        break

    else:

        log.error('Failed to bring the system online.')
        log.info('Sending notification to the operator...')
        sendMsg('System is offline!', 'Controle.Operator')

def requestRobot(msg, ctx):

    type_req = msg.function

    if type_req != 'FALSE FUNCTION':
       log.info(f'{msg.function} request received from OPERATOR...')
       log.info(f'Sending {msg.function} request to ROBOT CONTROLLER...')

    subscription1 = Subscription(channel)
    request = Message(content=msg, reply_to=subscription1)

    if msg.function == 'GET POSITION':

       channel.publish(request, topic="Get.Position")

    elif msg.function == 'SET POSITION':

       log.info(f'ROBOT ID: {msg.id} - X: {msg.positions.x} - Y: {msg.positions.y}')
       channel.publish(request, topic="Set.Position")

    else:

       log.info(f'Received a INVALID ARGUMENT request from OPERATOR')
       return Status(StatusCode.INVALID_ARGUMENT, 'Function must be get_position or set_position')

    try:


       if type_req != 'FALSE FUNCTION':
          log.info(f'Waiting {type_req} reply from ROBOT CONTROLLER...')
          log.info(f'{type_req} from ROBOT CONTROLLER received:')

       reply = channel.consume()
       requisicaoRobo = RequisicaoRobo()
       requisicaoRobo = reply.unpack(RequisicaoRobo)

       if type_req == 'GET POSITION':

          log.info(f'ROBOT ID: {requisicaoRobo.id} - X: {requisicaoRobo.positions.x} - Y: {requisicaoRobo.positions.y}')
          log.info('Sending GET POSITION reply to OPERATOR...')

       elif type_req == 'SET POSITION':

          log.info(f'{type_req} Reply: {reply.status.code}')

       else:

          log.info('Received a INVALID ARGUMENT request from OPERATOR...') 

       return requisicaoRobo

    except socket.timeout:
       log.error('No reply :(')
       return None


log.info('Creating the RPC Server and waiting for requests...')

provider = ServiceProvider(channel)

provider.delegate(
    topic="Controle.requests",
    function=requestRobot,
    request_type=RequisicaoRobo,
    reply_type=RequisicaoRobo
) 

provider.run()

