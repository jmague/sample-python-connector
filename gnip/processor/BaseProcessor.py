import multiprocessing
import multiprocessing.queues
import logging

class BaseProcessor(object):
    def __init__(self, upstream, environment):
        self.environment = environment
        self.queue = upstream
        self._stopped = multiprocessing.Event()
        self.run_process = multiprocessing.Process(target=self._run)
        self.environment.logr.debug("Base processor initialized")
        
    def run(self):
        self.run_process.start()

    def _run(self):
        while not self._stopped.is_set():
            msg = self.queue.get()
            print(str(msg))

    def stop(self):
        self.environment.logr.debug("stoping processor")
        self._stopped.set()

    def running(self):
        self.run_process.is_alive() and not self._stopped.is_set()

    def stopped(self):
        return self._stopped.is_set() and self.queue.qsize() == 0
