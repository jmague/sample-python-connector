import multiprocessing
import os
import time
import ujson

class FileProcessor(object):
    def __init__(self, upstream, environment):
        self.environment = environment
        self.queue = upstream
        self._stopped = multiprocessing.Event()
        self.run_process = multiprocessing.Process(target=self._run)
        self.environment.logr.debug("File processor initialized")
        self.current_tweet_file = None
        self.current_deleted_tweets_file = None
        self.current_retweet_file = None
        self.data_path=environment.config.get('process', 'baseDataDir')
        try:
            os.mkdir(self.data_path)
        except OSError:
            # File exists
            pass
        
        
    def run(self):
        self.run_process.start()

    def _run(self):
        try:
            while not self._stopped.is_set():
                if not self.queue.empty():
                    tweet = self.queue.get()
                    self.processTweet(tweet)
                else:
                    time.sleep(1)
            if not self.queue.empty():
                for tweet in self.queue.get(block=False): #queue is emptyed before leaving
                    self.checkDataFile(tweet)
                    self.current_tweet_file.write(json.dumps(tweet, default = json_date_handler)+'\n')
            
        except Exception as e:
            self.environment.logr.error("error in file processor: %s"%str(e))
            raise e
        finally:
            self._stopped.set() 
            if not(self.current_tweet_file is None or self.current_tweet_file.closed):
                self.environment.logr.debug("closing data file %s" % self.current_tweet_file.name)
                self.current_tweet_file.close()
            else:
                self.environment.logr.debug("no file open, no file to close")
    
    def stop(self):
        self.environment.logr.debug("stoping processor")
        self._stopped.set()

    def running(self):
        return self.run_process.is_alive() and not self._stopped.is_set()

    def stopped(self):
        return self._stopped.is_set() and self.queue.qsize() == 0
        
    def checkDataFile(self,date,type):
        if type=='tweet':
            if self.current_tweet_file is None or self.current_tweet_file.closed:
                self.current_tweet_file=open("%s/%s.data"%(self.data_path,date),'a')
                self.environment.logr.debug("opening %s" % self.current_tweet_file.name)
            if self.current_tweet_file.name != "%s/%s.data"%(self.data_path,date):
                self.environment.logr.debug("Tweets file switching")
                self.environment.logr.debug("Closing %s" % self.current_tweet_file.name)
                self.current_tweet_file.close()
                self.current_tweet_file=open("%s/%s.data"%(self.data_path,date),'a')
                self.environment.logr.debug("Opening %s" % self.current_tweet_file.name)
        if type=='retweet':
            if self.current_retweet_file is None or self.current_retweet_file.closed:
                self.current_retweet_file=open("%s/%s.data.retweets"%(self.data_path,date),'a')
                self.environment.logr.debug("opening %s" % self.current_retweet_file.name)
            if self.current_retweet_file.name != "%s/%s.data.retweets"%(self.data_path,date):
                self.environment.logr.debug("Retweets file switching")
                self.environment.logr.debug("Closing %s" % self.current_retweet_file.name)
                self.current_retweet_file.close()
                self.current_retweet_file=open("%s/%s.data.retweets"%(self.data_path,date),'a')
                self.environment.logr.debug("Opening %s" % self.current_retweet_file.name)
        if type=='delete':
            if self.current_deleted_tweets_file is None or self.current_deleted_tweets_file.closed:
                self.current_deleted_tweets_file=open("%s/%s.delete"%(self.data_path,date),'a')
                self.environment.logr.debug("opening %s" % self.current_deleted_tweets_file.name)
            if self.current_deleted_tweets_file.name != "%s/%s.delete"%(self.data_path,date):
                self.environment.logr.debug("Deleted tweets file switching")
                self.environment.logr.debug("Closing %s" % self.current_deleted_tweets_file.name)
                self.current_deleted_tweets_file.close()
                self.current_deleted_tweets_file=open("%s/%s.delete"%(self.data_path,date),'a')
                self.environment.logr.debug("Opening %s" % self.current_deleted_tweets_file.name)

        
        
    
    def processMessage(self,message):
        if 'objectType' in message and message['objectType']=='activity':
            if message['verb']=='post':
                processTweet(message)
            elif message['verb']=='share':
                processRetweet(message)
            elif message['verb']=='delete':
                processDeletedTweet(message)
        else:
            self.environment.logr.error('unknow message: %s'%ujson.dumps(message))        
            
    def processTweet(self,tweet):
        self.checkDataFile(tweet['postedTime'].split(':')[0], 'tweet')
        self.current_tweet_file.write(ujson.dumps(tweet)+'\n')

    def processRetweet(self,tweet):
        self.environment.logr.critical("Retweet received!!!")
        self.environment.logr.critical(ujson.dumps(tweet))
        self.checkDataFile(retweet['postedTime'].split(':')[0], 'retweet')
        self.current_retweet_file.write(ujson.dumps(tweet)+'\n')

    def processDeletedTweet(self,tweet):
        self.checkDataFile(tweet['postedTime'].split(':')[0], 'delete')
        self.current_deleted_tweet_file.write(ujson.dumps(tweet)+'\n')
