
import gevent.monkey
gevent.monkey.patch_all()

import gevent

from gevent.event import Event
from urllib.request import urlopen
from gevent.pool import Pool

evt = Event()
url = "http://slowwly.robertomurray.co.uk/delay/3000/url/https:/www.python.org/"

# Deterministic Gevent Pool
p1 = Pool(10)

def download(pid):
    response = urlopen(url) 
    size = response.headers['Content-Length']                
    if  p1.free_count() == 9:
      evt.set()
    print("Tasks ID:" + str(pid) + "  - Size:" + str(size) +  " KB")
    return size
    
def calculate_pi():
    pi = 0
    accuracy = 100000
    for i in range(0, accuracy):
        pi += ((4.0 * (-1)**i) / (2 * i + 1))        
    print("Waiting for downloads to complete")
    evt.wait()
    print("Caculated PI value " + str(pi))
    
def asynchronous():
    runList = []
    gevent.spawn(calculate_pi)    
    runList = [a for a in p1.imap_unordered(download, range(1,11))]    

    for x in range(len(runList)):
      print(runList[x])
    print("Size of the all tasks response is same - " + str(all(x==runList[0] for x in runList)))   


asynchronous()
