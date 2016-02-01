__author__ = "Nick Isaacs"
import configparser
import os
import logging.handlers
import logging
import sys

RELATIVE_CONFIG_PATH = "../config/gnip.cfg"


class Envirionment(object):
    def __init__(self):
        # Just for reference, not all that clean right now
        self.config_file_name = None
        self.config = None
        self.setup_config()
        self.streamname = self.config.get('gnip', 'streamName')
    
        self.logr = None
        self.rotating_handler = None #shared across modules
        self.setup_logs()
        self.logr.info("readding configuration file: %s"%self.config_file_name)
        
        
        self.username = self.config.get('gnip', 'userName')
        self.password = self.config.get('gnip', 'password')
        self.streamurl = self.config.get('gnip', 'streamURL')
        try:
            self.compressed = self.config.getboolean('gnip', 'compressed')
        except configparser.NoOptionError:
            self.compressed = True

    
        
    def setup_logs(self):
        self.logr = logging.getLogger(__name__)
        
        self.logfilepath = self.config.get('logger', 'logFilePath').strip(r'^/') or "log"
        try:
            os.mkdir(self.logfilepath)
        except OSError:
            # File exists
            pass
        
        logging_level={'CRITICAL': logging.CRITICAL, 'ERROR': logging.ERROR, 'WARNING': logging.WARNING, 'INFO': logging.INFO, 'DEBUG': logging.DEBUG}
        self.logr.setLevel(logging_level[self.config.get('logger', 'logLevel').strip(r'^/').upper() or 'DEBUG'])
        
        if self.logr.level>=logging.INFO:
            formatString="%(asctime)s: %(message)s"
        else:
            formatString="%(asctime)s [%(levelname)s] [%(module)s.%(funcName)s]: %(message)s"
        
        self.rotating_handler = logging.handlers.RotatingFileHandler(
            filename=self.logfilepath + "/%s-log" % self.streamname,
            mode='a', maxBytes=2 ** 24, backupCount=5)
        self.rotating_handler.setFormatter(logging.Formatter(formatString))
        self.logr.addHandler(self.rotating_handler)
        

    def setup_config(self):
        if 'GNIP_CONFIG_FILE' in os.environ:
            self.config_file_name = os.environ['GNIP_CONFIG_FILE']
        else:            
            dir = os.path.dirname(__file__)
            self.config_file_name = os.path.join(dir, RELATIVE_CONFIG_PATH)
            if not os.path.exists(self.config_file_name):
                self.logr.debug("No configuration file found.")
                sys.exit()
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file_name)
