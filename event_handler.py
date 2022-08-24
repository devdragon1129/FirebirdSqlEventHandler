import fdb
import threading
from threading import Thread
import time
from itertools import groupby
from operator import itemgetter
import queue
import configparser
config = configparser.ConfigParser()
config.read('config.ini')

con = fdb.connect( host=config['Configuration']['HOST'], database=config['Configuration']['DB_PATH'],
    user=config['Configuration']['USER'], password=config['Configuration']['PASSWORD'])
cur = con.cursor()



# global variable list
inicio = time.time()


# function to send messages by telegram:
def eventhook(q):
    
    while True:
        try:
            event_id = q.get(block=False)
        except queue.Empty:
            break
        time.sleep(1)
        try:
            cur.execute(""" select * from event_status where event_id = '%s' """ % event_id)
            row1 = cur.fetchall()
        except BaseException as error:
            print(
                "in time an error occurred with the information in the database", error)
        else:
            result = []
            for k, g in groupby(row1, itemgetter(0)):
                g = list(g)           # Convert the group to a list
                record = list(g[0])   # Copy the first record of the group
                result.append(tuple(record))
                for j in range(len(result)):
                    # assign variables to database query results
                    event_id, e_datetime, event, station, status, objectname, address = result[
                        j][0:7]
                    msgFromServer = str(event) + ' occured at ' + str(e_datetime) + ' in station ' + str(station) + \
                        ' and status of it is ' + str(status) 
                    print(msgFromServer, "\n")
                    alarmas.discard(event_id)
                    break
                break
q = queue.Queue()

alarmas = set()
while True:
    with con.event_conduit(['new_events_inserted']) as conduit:
        events = conduit.wait()
        try:
            sql1 = "select first 1 event_id from event_status order by e_datetime desc"
            cur.execute(sql1)
            row = cur.fetchall()
            for h in row:
                event_id = h[0]                
        except BaseException as error:
            print(
                "in event an error occurred with the database information", error)
        else:
            if event_id not in alarmas:
                q.put(event_id)
                alarmas.add(event_id)
                n_threads = threading.active_count()
                if n_threads < 3:
                    threadID = Thread(target=eventhook, args=(q,))
                    threadID.start()