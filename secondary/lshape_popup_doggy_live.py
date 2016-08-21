
import socket, os, string
import MySQLdb, select, sys
import thread, time, struct, decimal
from datetime import datetime
import xml.etree.ElementTree as ET
import datetime




buddyplaylist = []

serverip = '10.3.10.191'
serverdbname = 'casparcg'
serverdbpass = 'password'
serverdbuser = 'root'
servername = 'secondary'
sock = 0
sock_gfx = 0
program_type = ''
getCommand = ''

def CreateClient():
    sock = socket.create_connection(('localhost', 5250))
    return sock

def CreateGFXConnection():
    sock = socket.create_connection(('10.3.10.195', 5250))
    return sock

def SendToServer(fd, cmd):
    fd.send(cmd)
    
def FetchPlayList():
    global program_type
    myplaylist = 0
    program_type = ''
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    
    sql = "SELECT scheduler_id,label FROM playlist_1 WHERE state='1'"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        
        for r in results:
            myplaylist = int(r[0])
            program_type = str(r[1])
        # mark all the items as not played
        # for i in results
    except:
        print "Failed To FetchPlayList From DB"
        pass
    db.close()
    return myplaylist


def FetchCommercial():
    global program_type
    id = FetchPlayList()
    myplaylist = []
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    
    sql = "SELECT scheduler_id,commercial,type,state,duration,time FROM lshape WHERE program_id= %d and state = 0 order by time asc" % (id)
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        
        for r in results:
            myplaylist.append(str(r[0]) +'|'+ str(r[1]) + '|' + str(r[2]) + '|' + str(r[3]) + '|' + str(r[4]) + '|' + str(r[5]))
            
        # mark all the items as not played
        # for i in results
    except:
        print "Failed To FetchPlayList From DB"
        pass
    db.close()
    return myplaylist
    
def LshapeHandler():
    global sock,program_type
    currlist = ''
    nowTime = ''
    myplaylist = []
    sleeptime = 0
    t1 = 0
    t2 = 0
    sock = CreateClient()
    while True:
        try:
            currlist = myplaylist.pop(0)
        except:
            myplaylist = FetchCommercial()
            t1 = 0
            t1 = time.time()
            t2 = 0
            if len(myplaylist) == 0:
                nowTime = str(datetime.datetime.now())
                print nowTime+" waiting for commercial to play ..."
                time.sleep(0.5)
            continue
        
        
        
       
        
        scheduler_id,commercial,type,state,duration,start_time = currlist.split("|")
        hr,min,sec,frame = duration.split(':')
        sleeptime = float(sec) + (float(frame)/25)
        if sleeptime > 0:
            while True:
                t2 = time.time()
                if (t2 - t1) >= float(start_time):
                    print (t2 - t1)
                    if program_type.upper() == 'PGM':
                        if type.upper() == 'LSHAPE':
                            
                            pcmd = 'play 1-9 "%s" auto\r\n' % (commercial)
                            SendToServer(sock, pcmd)
                            nowTime = str(datetime.datetime.now())
                            print nowTime+" "+pcmd
                            PlayedLog(scheduler_id,type,commercial,str(sleeptime*25),nowTime)
                            pcmd = "MIXER 1-10 FILL .25 0 .75 .75 25 easeinsine\r\n"
                            SendToServer(sock, pcmd)
                            time.sleep(sleeptime)
                            pcmd = "MIXER 1-10 FILL 0 0 1 1 25 easeinsine"
                            SendToServer(sock, pcmd)
                            pcmd = "MIXER 1-10 CLEAR\r\n"
                            SendToServer(sock, pcmd)
                            print str(datetime.datetime.now())+" "+pcmd
                        elif type.upper() == 'POPUP' or type.upper() == 'DOGGY':
                            pcmd = 'play 1-14 "%s" auto\r\n' % (commercial)
                            SendToServer(sock, pcmd)
                            nowTime = str(datetime.datetime.now())
                            print nowTime+" "+pcmd
                            PlayedLog(scheduler_id,type,commercial,str(sleeptime*25),nowTime)
                    else:
                        if type.upper() == 'LSHAPE':
                            
                            pcmd = 'play 1-9 "%s" auto\r\n' % (commercial)
                            SendToServer(sock, pcmd)
                            nowTime = str(datetime.datetime.now())
                            print nowTime+" "+pcmd
                            PlayedLog(scheduler_id,type,commercial,str(sleeptime*25),nowTime)
                            pcmd = "MIXER 1-10 FILL .25 0 .75 .75 25 easeinsine\r\n"
                            SendToServer(sock, pcmd)
                            time.sleep(sleeptime)
                            pcmd = "MIXER 1-10 FILL 0 0 1 1 25 easeinsine"
                            SendToServer(sock, pcmd)
                            pcmd = "MIXER 1-10 CLEAR\r\n"
                            SendToServer(sock, pcmd)
                            print str(datetime.datetime.now())+" "+pcmd
                        elif type.upper() == 'POPUP':
                            pcmd = 'play 1-14 "%s" auto\r\n' % (commercial)
                            SendToServer(sock, pcmd)
                            nowTime = str(datetime.datetime.now())
                            print nowTime+" "+pcmd
                            PlayedLog(scheduler_id,type,commercial,str(sleeptime*25),nowTime)
                            pcmd = "MIXER 1-10 FILL 0 0 1 .75 25 easeinsine\r\n"
                            SendToServer(sock, pcmd)
                            time.sleep(sleeptime)
                            pcmd = "MIXER 1-10 FILL 0 0 1 1 25 easeinsine"
                            SendToServer(sock, pcmd)
                            pcmd = "MIXER 1-10 CLEAR\r\n"
                            SendToServer(sock, pcmd)
                            print str(datetime.datetime.now())+" "+pcmd
                        elif type.upper() == 'DOGGY':
                            pcmd = 'play 1-14 "%s" auto\r\n' % (commercial)
                            SendToServer(sock, pcmd)
                            nowTime = str(datetime.datetime.now())
                            print nowTime+" "+pcmd
                            PlayedLog(scheduler_id,type,commercial,str(sleeptime*25),nowTime)
                    break
        UpdateDb(commercial, scheduler_id, '2')
        

def UpdateDb(name, scheduler_id, state):
    global isbuddydbrunning
    db = MySQLdb.connect(serverip, serverdbuser, serverdbpass, serverdbname)
    cursor = db.cursor()
    sql = "UPDATE lshape SET state='%s' where commercial='%s' and scheduler_id='%s'" % (state, name, scheduler_id)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        print "DB update failed"
    db.close()

def PlayedLog(scheduler_id,label,name,duration,played_time):
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    sql = "insert into alternative_log (scheduler_id,program_type,name,duration,played_time)\
           values (%d,'%s','%s','%s','%s')" % (int(scheduler_id),label,name,duration,played_time)
    
    try:
        cursor.execute(sql)
        db.commit()
    except:
        pass
    db.close()

def serverCaspar():
    global getCommand 
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 50009))
    while True:
        s.listen(1)
        conn, addr = s.accept()
        data = conn.recv(1024)
        if not data: continue
        getCommand = data
    conn.close()
    
if __name__ == "__main__":
    thread.start_new_thread(serverCaspar,())
    LshapeHandler()
    #print FetchCommercial()
