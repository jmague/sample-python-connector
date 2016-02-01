#!/usr/bin/env python
__author__ = 'scott hendrickson, nick isaacs'
import time
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
try:
    import http.client as httplib
except ImportError:
    import httplib
import ssl
import base64
import zlib
import socket
import logging
import os
import traceback

from src.utils.Envirionment import Envirionment
from multiprocessing import Event, Process, Manager, Queue
from ctypes import c_char_p

CHUNK_SIZE = 2 ** 17  # decrease for v. low volume streams, > max record size
GNIP_KEEP_ALIVE = 30  # 30 sec gnip timeout
MAX_BUF_SIZE = 2 ** 22  # bytes records to hold in memory
MAX_ROLL_SIZE = 2 ** 30  # force time-period to roll forward
DELAY_FACTOR = 1.5  # grow by DELAY_FACTOR - 1 % with each failed connection
DELAY_MAX = 150  # maximum delay in seconds
DELAY_MIN = 0.1  # minimum delay in seconds
DELAY_RESET = 60 * 10  # Connected for the long, then reset the delay to min
NEW_LINE = '\r\n'


class GnipRawStreamClient(object):
    def __init__(self, environment):
        self.environment=environment
        self.compressed = self.environment.compressed
        self.streamName = self.environment.streamname
        self.streamURL = self.environment.streamurl
        self.headers = {'Accept': 'application/json',
                        'Connection': 'Keep-Alive',
                        'Accept-Encoding': 'gzip',
                        'Authorization': 'Basic %s' % base64.encodebytes(bytes('%s:%s' % (self.environment.username, self.environment.password), 'utf8')).replace(b'\n',b'').decode("utf-8")
        }
        self._stop = Event()
        delay_reset = time.time()
        delay = DELAY_MIN
        self.run_process = Process(target=self._run, args=(delay, delay_reset))
        self.buffer = ""
        self.queue = Queue()
        self.environment.logr.debug("Raw client initialized.")

    def running(self):
        return not self.stopped() and not ("" == self.get_string_buffer()) and not self.run_process.is_alive()

    def _run(self, delay, delay_reset):
        self.environment.logr.info("Raw client is running")
        while not self._stop.is_set():
            try:
                self.get_stream()                
                delay = DELAY_MIN
            except ssl.SSLError as e:
                delay = delay * DELAY_FACTOR if delay < DELAY_MAX else DELAY_MAX
                self.environment.logr.error("Connection failed: %s (delay %2.1f s)" % (e, delay))
            except httplib.IncompleteRead as e:
                self.environment.logr.error("Streaming chunked-read error (data chunk lost): %s" % e)

            except urllib2.HTTPError as e:
                self.environment.logr.error("HTTP error: %s" % e)

            except urllib2.URLError as e:
                delay = delay * DELAY_FACTOR if delay < DELAY_MAX else DELAY_MAX
                self.environment.logr.error("URL error: %s (delay %2.1f s)" % (e, delay))
            except socket.error as e:
                # Likely reset by peer (why?)
                delay = delay * DELAY_FACTOR if delay < DELAY_MAX else DELAY_MAX
                self.environment.logr.error("Socket error: %s (delay %2.1f s)" % (e, delay))
            if time.time() - delay_reset > DELAY_RESET:
                # if we have been connected for a long time before this error,
                # then reset the delay
                delay = DELAY_MIN
            delay_reset = time.time()
            time.sleep(delay)
        self.environment.logr.info("Raw client is stopped")

    def run(self):
        self.run_process.start()

    def stop(self):
        self.environment.logr.debug("stopping Raw client")
        self._stop.set()

    def get_stream(self):
        self.environment.logr.info("Connecting to stream: %s"%self.streamURL)
        req = urllib2.Request(self.streamURL, headers=self.headers)
        response = urllib2.urlopen(req, timeout=(1 + GNIP_KEEP_ALIVE))
        self.environment.logr.info("Connected to stream.")
        # sometimes there is a delay closing the connection, can go directly to the socket to control this
        realsock = response.fp.raw._sock
        try:
            decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            while not self._stop.is_set():
                chunk = response.read(CHUNK_SIZE)
                if self.compressed:
                    chunk = decompressor.decompress(chunk)
                chunk = chunk.decode("utf-8")
                if chunk == '':
                    return
                
                self.buffer+=chunk
                
                self.buffer.replace("}{", "}%s{" % NEW_LINE)
                lines = self.buffer.split(NEW_LINE)
                self.buffer=lines[-1]
                for line in lines[:-1]:
                    self.queue.put(line)                        
                
        except Exception as e:
            self.environment.logr.error("Buffer processing error (%s) - restarting connection" % e)
            realsock.close()
            response.close()
            raise e

    def stopped(self):
        self._stop.is_set()
