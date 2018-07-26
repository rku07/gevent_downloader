#!/usr/bin/env python
# -*- coding: utf-8 -*

"""Using gevent to download and calculate the pi at the same time.
Detailed description :
Download data from the <URL> <N> times as fast as possible. At the same time as performing the downloads, 
you are to calculate the value of PI as accurately as possible. As soon as all the URLs have been 
downloaded, the same block of code that calculated PI is to display its final value, followed by each 
download task displaying the number of bytes downloaded and some mechanism to show a human reader that 
the data is the same for each download.
"""

__author__ = 'rku'
__version__ = "1.0"
__maintainer__ = "Ramesh Kumar"
__email__ = "ramesh.kumar@sky.uk"
__status__ = "Development"

#import gevent
from gevent.queue import Queue
from gevent.event import Event
from gevent.pool import Group
import urllib2
import sys
import traceback
import time
import logging
#import random

import gevent.monkey
gevent.monkey.patch_all()

TASK_COUNT=10
evt = Event()
msg_queue = Queue()
url = "http://slowwly.robertomurray.co.uk/delay/3000/url/https://www.python.org/"
#url = "https://www.python.org/"
#urls = ["https://www.python.org/", "https://www.oracle.com/"]
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
 

class DownloadedData(object):
    """
    Holds the value for each downloaded task
    """
    def __init__(self, size=0, url=None, data=None, task_id=None):
        self.url = url 
        self.data = data 
        self.size= size 
        self.task_id = task_id

    def set_size(self, value):
        self._size = value

    def get_size(self):
        return self._size
    
    def set_data(self, value):
        self._data = value

    def get_data(self):
        return self._data
    
    def set_task_id(self, value):
        self._task_id = value

    def get_task_id(self):
        return self._task_id

    def __eq__(self, other):
        return self.data == other.data

    size = property(get_size, set_size)
    data = property(get_data, set_data)
    task_id = property(get_task_id, set_task_id)

def download(task_id):
    """
    function for downloading from the URL and puts object into the Queue
    """
    try:
        response  = urllib2.urlopen(url)
    except Exception as e:
        logger.info( "Exception raised:{} {}".format(e, traceback.format_exc()))
        sys.exit(1)

    # response  = urllib2.urlopen(urls[random.randint(0,1)])
    downloaded_data = DownloadedData()
    downloaded_data.size = response.info().get('Content-Length')
    downloaded_data.data = response.read() 
    downloaded_data.task_id =  task_id
    msg_queue.put(downloaded_data)
    logger.info('Task completed : {}'.format(task_id))    
    if msg_queue.qsize() == TASK_COUNT:
        logger.info( "-"*100)
        logger.info("All Task completed, Waking up displayer")
        evt.set()
        logger.info( "-"*100)
    

def asynchronous():
    """
    Creates a group of threads to do the parallel download and pi calculation
    """
    download_group = Group()
    calculate_pi_group=Group()    
    
    for task_id in range(1,11):
        download_group.add(gevent.spawn(download, task_id))
        #[a for a in download_group.imap_unordered(download, task_id)]

    calculate_pi_group.add(gevent.spawn(displayer))
    calculate_pi_group.join()
    download_group.join()
    #gevent.joinall(threads)

def calculate_pi():
    """
    calculating pi using Leibniz Formula
    """
    pi = 0
    accuracy = 100000
    for i in range(0, accuracy):
        pi += ((4.0 * (-1)**i) / (2 * i + 1))

    return pi

def displayer():
    """
    This is our main function which does the following:
      - calculate the pi and displays it
      - waiting for the download to complete
      - determining the downloaded data 
    """
    logger.info("Waiting for the download to complete(calculating pi)...")
    result = []
    value_of_pi = calculate_pi()  
    evt.wait()
    while not msg_queue.empty():
        data_from_queue = msg_queue.get()
        logger.info("Task :{}, Bytes Downloaded: {}".format(data_from_queue.task_id, 
                                                            data_from_queue.size))
        result.append(data_from_queue)

    logger.info( "-"*100)
    logger.info("The value of PI is :{}".format(value_of_pi))
    logger.info( "-"*100)
    
    is_all_equal =  all (item==result[0] for item in result) 
    if is_all_equal:
        logger.info("Data is the *Same* for each download")
    else:
        logger.info("Data is *NOT* the same for each download")

        from collections import defaultdict
        d = defaultdict(list)
        for item in result:
            d[item.size].append(item)

        for key in d.keys():
            logger.info('-'*100)
            logger.info('Downloaded Size:{}b'.format(key))
            logger.info('Tasks:{}'.format(','.join(str(item.task_id) for item in d[key])))
            logger.info('-'*100)

if __name__ == '__main__':
    try:
        start_time = time.time()
        asynchronous()
    except Exception as e:
        logger.info( "Exception raised:{}".format(traceback.format_exc()))
        sys.exit(1)
    time_taken = time.time() - start_time
    logger.info( "Total time taken:{}s".format(time_taken))

