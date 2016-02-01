import ujson
import logging
import multiprocessing
import time
from GnipRawStreamClient import GnipRawStreamClient


class GnipJsonStreamClient(object):
    def __init__(self, environment):
        self.environment=environment
        self.gnip_raw_sream_client = GnipRawStreamClient(self.environment)
        self.producer_queue = multiprocessing.Queue()
        self._stop = multiprocessing.Event()
        self.run_thread = multiprocessing.Process(target=self.process_raw_queue)
        self._started = False
        self.environment.logr.debug("Json client initialized.")

    def started(self):
        return self._started

    def run(self):
        self.gnip_raw_sream_client.run()
        self.run_thread.start()
        self.environment.logr.debug("Json client is runnnig")

    def running(self):
        return not self.stopped()

    def stop(self):
        self.environment.logr.debug("stopping Json client")
        self._stop.set()
        self.gnip_raw_sream_client.stop()
        self.environment.logr.debug("Json client is stopped")

    def stopped(self):
        return self._stop.is_set()

    def queue(self):
        return self.producer_queue
        
    def process_raw_queue(self):
        while not self.stopped():
            try:
                if not self.gnip_raw_sream_client.queue.empty():
                    rawTweet=self.gnip_raw_sream_client.queue.get()
                    jsonTweet=ujson.loads(rawTweet)
                    self.producer_queue.put(jsonTweet)
                else:
                    time.sleep(1)
            except ValueError:
                self.environment.logr.error("There was a ValueError in the rawTweet: " + rawTweet)
            except Exception as e:
                self.environment.logr.error("There was an error: %s"%str(e))
                self.stop()
                raise e
        
        
