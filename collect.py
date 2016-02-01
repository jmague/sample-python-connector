from gnip.GnipJsonStreamClient import GnipJsonStreamClient
from gnip.FileProcessor import FileProcessor
from gnip.Envirionment import Envirionment
import time

environment = Envirionment()

client = GnipJsonStreamClient(environment)
processor = FileProcessor(client.queue(), environment)
  
try:
    client.run()
    processor.run()
    while client.running() and processor.running():
        time.sleep(1)
except KeyboardInterrupt:
    environment.logr.info('Shutting collection down...')
except Exception as e:
    environment.logr.error('Error!')
    raise e
environment.logr.debug('main loop left')
while client.running() or processor.running():
    if client.running():
        environment.logr.debug('stopping client')
        client.stop()
    if processor.running():
        environment.logr.debug('stopping processor')
        processor.stop()
