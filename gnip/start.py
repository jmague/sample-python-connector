from utils import Envirionment
import sys
import Queue
import logging
from processor import *
from stream    import GnipJsonStreamClient
from metrics   import Metrics

logr = logging.getLogger('Enviroinment Logger')

def processors_for_queue(config, queue):
    processors = []
    if config.processtype == "latency":
        processors.append(Latency(queue))
    elif config.processtype == "files":
        processors.append(SaveThread(queue))
    elif config.processtype == "files-gnacs":
        processors.append(SaveThreadGnacs(queue))
    elif config.processtype == "rules":
        processors.append(CountTwitterRules(queue))
    elif config.processtype == "redis":
        processors.append(Redis(queue))
    elif config.processtype == "fileandmetrics":
        if "sql_db" not in config.kwargs:
            config.logr.error("No database configured.")
            sys.exit()
        processors.append(SaveThread(queue))
        processors.append(Metrics(queue))
    else:
        logr.error("No valid processing strategy selected (%s), aborting"%config.processtype)
        sys.exit(-1)
    return processors


configuration = Envirionment()
queue = Queue.Queue()
client = GnipJsonStreamClient(
    configuration.streamurl,
    configuration.streamname,
    configuration.username,
    configuration.password,
    configuration.filepath,
    configuration.rollduration,
    compressed=configuration.compressed
    )

client.run()
self.logr.debug("Made it here")
for processor in processors_for_queue(queue):
    processor.run()