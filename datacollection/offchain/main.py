import _thread
import time
from dlcollectors import DLCollector

# TODO: introduce decorators to do the timing thing...
# https://realpython.com/primer-on-python-decorators/

# TODO: Threads can die. Make sure everything keeps running with exceptions?

def collect_server_realms( threadName, collector, delay):
    while True:
        try:
            timestamp = int(time.time())
            if((timestamp % delay) == 0):
                servers = collector.getServers()
                realms = collector.getRealms()
                server_realms = realms[['url', 'serverName', 'usersCount', 'maxUsers']].rename(columns={"url":"baseUrl"}).merge(servers, on="baseUrl")
                server_realms.to_sql('servers', collector.dbEngine, if_exists='append',index=False)
                print ("%s completed at: %s" % ( threadName, time.ctime(time.time()) ))
                time.sleep(1)
            else:
                time.sleep(0.3)
        except Exception as e:
            print(threadName, "failed with", e)


def collect_positions( threadName, collector, delay):
    while True:
        try:
            timestamp = int(time.time())
            if((timestamp % delay) == 0):
                userPos = collector.getAllServerUserPositions()
                userPos.to_sql('positions', collector.dbEngine, if_exists='append',index=False)
                print ("%s completed at: %s" % ( threadName, time.ctime(time.time()) ))
                time.sleep(1)
            else:
                time.sleep(0.3)
        except Exception as e:
            print(threadName, "failed with", e)

def collect_events( threadName, collector, delay):
    while True:
        try:
            timestamp = int(time.time())
            if((timestamp % delay) == 0):
                events = collector.getEvents()
                events.to_sql('events', collector.dbEngine, if_exists='append',index=False)
                print ("%s completed at: %s" % ( threadName, time.ctime(time.time()) ))
                time.sleep(1)
            else:
                time.sleep(0.3)
        except Exception as e:
            print(threadName, "failed with", e)

def collect_profiles( threadName, collector, delay):
    while True:
        try:
            timestamp = int(time.time())
            if((timestamp % delay) == 0):
                profiles = collector.getProfiles()
                profiles.to_sql('profiles', collector.dbEngine, if_exists='append',index=False)
                print ("%s completed at: %s" % ( threadName, time.ctime(time.time()) ))
                time.sleep(1)
            else:
                time.sleep(0.3)
        except Exception as e:
            print(threadName, "failed with", e)


# Create two threads as follows
try:
    col = DLCollector()
    _thread.start_new_thread(collect_server_realms, ("Server Collector", col, 120)) # 24*60*60
    _thread.start_new_thread(collect_positions, ("Position Collector", col, 30)) # 60
    _thread.start_new_thread(collect_events, ("Event Collector", col, 60)) # 60*5
    _thread.start_new_thread(collect_profiles, ("Profile Collector", col, 90) )
except Exception as e:
   print ("Error: unable to start thread", e)

while 1:
   pass
