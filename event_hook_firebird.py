import fdb
import threading
from threading import Thread, Lock
import telegram
#import pywhatkit as kit
import datetime
import time
from itertools import groupby
from operator import itemgetter
from twilio.rest import Client
from twilio.twiml.voice_response import Play, VoiceResponse, Say, Dial, Number
import queue

bot_token = '1464667317:AAGF1qjunnKbyibYoteqweqweqeqweeqw'
# chatid='1256819397'
con = fdb.connect(dsn='127.0.0.1:E:/Software/Firebird/COURIER.FDB',
                  user='sysdba', password='masterkey')
cur = con.cursor()



# global variable list
inicio = time.time()
chatid1 = '1256819397'  # iphone luciano
chatid2 = '76848454'  # nova
chatid3 = '1938022118'  # note4 galaxy
chatid4 = '107723150'  # note 7 xiaomi


# function to send messages by telegram:
def luk(notad, chatid):
    # if chatid != '':
    bot = telegram.Bot(token=bot_token)
    try:
        bot.sendMessage(chatid, text=notad)
    except BaseException as error:
        print("could not send message", error)

    try:
        bot.sendMessage(chatid2, text=notad)  # nova
    except BaseException as error:
        print("could not send message", error)

    try:
        # bot.sendMessage(chatid3, text=notad)  # luciano note4
        bot.sendMessage(chatid4, text=notad)  # luciano xiaomi
    except BaseException as error:
        print("could not send message", error)


# function to make calls by twilio:
def udpmessage(phone, msgFromClient):

    account_sid = 'ACee2f202ef9a1b3c71d5772af35b9caad'
    auth_token = '9f2abf6fbb46bf0a6f5512559379816a'
    client = Client(account_sid, auth_token)
    phone = '+1'+phone
    response = VoiceResponse()
    response.say(msgFromClient, voice="woman", language="es-MX", loop=2)
    #response.say(msgFromClient, voice="Polly.Miguel", loop=2)
    dial = Dial()
    dial.number(phone, status_callback_event='busy')
    print(phone)

    try:

        call = client.calls.create(
            # twiml='<Response><Say voice = "Alice" language = "es-MX"> HELLO, WE TALK TO YOU ABOUT' message '</Say>  <Play>http://luciano-casa.dyndns.biz:8080/mensajealarma.mp3</Play></Response>',
            # url='http://luciano-casa.dyndns.biz:8080/mensajealarma.mp3',
            twiml=response,
            to=phone,
            from_='+12057367682'
        )
        print(call.sid)

        #sock.sendto(response_bytes, address)
    except BaseException as error:
        print("the call could not be made", error)


def eventhook(q):
    
    while True:
        try:
            event_id = q.get(block=False)
            # print("Event Occured!")
        except queue.Empty:
        #    print("the queue is empty")
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
                # And now replace in that record fields 3 and 4
                # <----- HERE is added set()
                #record[3] = "ROBO " + \
                #    " y ".join(set(t[3].replace("ROBO", "") for t in g))
                #record[4] = max(t[4] for t in g)
                # Add that record (as a tuple) to the result
                result.append(tuple(record))

                for j in range(len(result)):

                    # assign variables to database query results
                    event_id, e_datetime, event, station, status, objectname, address = result[
                        j][0:7]

                    #if chatid is None:
                    #    chatid = '1938022118'
                    #elif chatid == '':
                    #    chatid = '1938022118'
                    #else:
                    #    print("chatid")
                    #    print(chatid)

                    # create message that will be sent to the client and the one that will be given in voice message by twilio
                    #msgFromClient = 'COMPU ALARMA 809-547-8958 ALARMA DE ' + \
                    #    str(cliente) + ' ' + \
                    #    str(eventodetalle) + '  FECHA ' + \
                    #    str(fecha.strftime("%d-%m-%Y  HORA %I:%M:%S %p"))
                    msgFromServer = str(event) + ' occured at ' + str(e_datetime) + ' in station ' + str(station) + \
                        ' and status of it is ' + str(status) 
                    print(msgFromServer, "\n")

                    # we send the message by telegram according to the bot id of the client and to the administration with the luk function
                    #try:
                    #    thread2 = Thread(name='thread%s' % event_id, target=luk,
                    #                   args=(msgFromClient, chatid, ))
                    #    thread2.start()
                    #    thread2.join()
                    #except BaseException as error:
                    #    print("could not send telegram", error)

                    # we make phone call by twilio with the function udpmessage
                    #try:
                    #    thread5 = Thread(name='thread%s' % event_id, target=udpmessage, args=(
                    #        tel1llamada, msgFromClient,))
                    #    thread5.start()
                    #    thread5.join()
                    #except BaseException as error:
                    #    print("call could not be made", error)

                    # completed we assign the value completed to the complete field of the database
                    #try:
                    #    print('This is the {}'.format(event_id))
                    #    cur.execute(
                    #        "update activas set COMPLETA = 'COMPLETADA' where completa = 'PENDIENTE' and event_id='{}'".format(event_id))
                        # cur.execute(
                        #     "update activas set COMPLETA = 'COMPLETADA' where completa = 'PENDIENTE' and event_id= '%s'", (event_id,))
                    #    con.commit()
                        # con.close()

                    #except BaseException as error:
                    #    print("the update could not be performed", error)

                    # we delete from the set called alarms the id of the client that we process
                    alarmas.discard(event_id)
                    break
                break


q = queue.Queue()

alarmas = set()
while True:
    with con.event_conduit(['new_events_inserted']) as conduit:
        events = conduit.wait()
        # print(events)
        try:
            # sql1 = "select first 1 csid from activas order by alarmnum desc"
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
                #print(alarmas)
                n_threads = threading.active_count()
                # print("active threads: ", n_hilos)
                if n_threads < 3:
                    threadID = Thread(target=eventhook, args=(q,))
                    threadID.start()

                #print("active threads: ", n_threads)
