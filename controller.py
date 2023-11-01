from is_wire.rpc import ServiceProvider, LogInterceptor
from is_wire.core import Channel, StatusCode, Status, Logger
from google.protobuf.empty_pb2 import Empty
from is_msgs.common_pb2 import Position
from RequisicaoRobo_pb2 import RequisicaoRobo
import time

log = Logger(name='Console')
empty = Empty()

class Robot():
    def __init__(self, id, x, y):

        self.id = id
        self.pos_x = x
        self.pos_y = y

    def get_id(self):
        return self.id

    def get_position(self):
        return self.pos_x, self.pos_y
    
    def set_position(self, x, y):

        self.pos_x = x
        self.pos_y = y

def get_position(msg, ctx):
   
    log.info('GET POSITION request received...')
    log.info('Validating arguments...')
    log.info('Robot found!')
    log.info(f'Robot - {robots[msg.id - 1].get_id()} - X: {robots[msg.id - 1].get_position()[0]} - Y: {robots[msg.id - 1].get_position()[1]}')

    requisicaoRobo = RequisicaoRobo()
    
    requisicaoRobo.id = robots[msg.id - 1].get_id()
    requisicaoRobo.function = 'GET POSITION'
    requisicaoRobo.positions.x, requisicaoRobo.positions.y = robots[msg.id - 1].get_position()

    log.info('Sending GET POSITION reply...')

    return requisicaoRobo

def set_position(msg, ctx):

    log.info(f'{msg.function} request received...')
    log.info('Validating arguments...')
    log.info('Robot found!')
    position = msg.positions

    if position.x < 0 or position.y < 0:
        return Status(StatusCode.OUT_OF_RANGE, 'The number must be positive')
    
    robots[msg.id - 1].set_position(x=position.x, y=position.y)

    log.info(f'Moving ROBOT ID: {msg.id} to X: {robots[msg.id-1].get_position()[0]} - Y: {robots[msg.id-1].get_position()[1]}')
    log.info(f'Sending SET POSITION reply...')

    return Empty()


log.info('Initializing Robots...')

robots = [Robot(id=1, x=1, y=2), Robot(id=2, x=5, y=3), Robot(id=3, x=7, y=6), Robot(id=4, x=9, y=1), Robot(id=1, x=6, y=1)]

for robot in robots:
   log.info(f'Robot:{robot.get_id()} - X: {robot.get_position()[0]} - Y: {robot.get_position()[1]}')

log.info('Creating Channel...')
log.info('Creating the RPC Server and waiting for Requests')

# Connect to the broker
channel = Channel('amqp://guest:guest@localhost:5672')

provider = ServiceProvider(channel)

provider.delegate(
    topic="Get.Position",
    function=get_position,
    request_type=RequisicaoRobo,
    reply_type=RequisicaoRobo
) 

provider.delegate(
    topic="Set.Position",
    function=set_position,
    request_type=RequisicaoRobo,
    reply_type=Empty
)

provider.run()


