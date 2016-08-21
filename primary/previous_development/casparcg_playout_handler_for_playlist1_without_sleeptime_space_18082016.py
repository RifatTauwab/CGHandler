#-*- coding: UTF-8 -*-
'''
CCClient.py - Written by Rakib Mullick <rakib.mullick@gmail.com> and Md.Rifat-Ut-Tauwab<mdruts@gmail.com>,
Sysnova Information Sys Ltd.
This program is written to send instruction to CasparCG Server; requests
will be fetched from database.
mysql db used - MySQL-python-1.2.4b4.win32-py2.7 taken from http://sourceforge.net/projects/mysql-python
'''

import socket, os, string
import MySQLdb, select, sys
import thread, time, struct, decimal
import datetime

_FLOAT_DGRAM_LEN = 4

myplaylist = []
buddyplaylist = []
buddyip=''
buddydbname = ''
buddydbpass = ''
buddydbuser = ''
serverip = ''
serverdbname = ''
serverdbpass = ''
serverdbuser = ''
servername = ''
scriptisrunning = 0
nowplaying = ''
logpath = ''
oscsock = 0
OscClientPort = 7250
layerpaused = False
buddynowplaying = ''
isbuddyplayingcommercial = 0
isRecovery = False
sock = 0
cgupdate = 0
isbuddydbrunning = True
logooff = False
cgrunning = False
osct1 = 0.0
osct2 = 0.0
liveStarted = 0
schedulerID = 0
buddyvideotimecode = ''
commercialtimelist = []
getLiveExitCommand = ''

class Logger(object):
    def __init__(self, filename="Default.log"):
        self.terminal = sys.stdout
        #self.error = sys.stderr
        self.log = open(filename, "a")

    def write(self, message):
        self.terminal.write(message)
        #self.error.write(message)
        self.log.write(message)

def CreateClient():
        sock = socket.create_connection(('localhost', 5250))
        return sock

def SendToServer(fd, cmd):
    fd.send(cmd)

def ConvertToSec(h, m, s):
    #print "ConvertToSec====>", h,m,s
    second = (int(h)*60*60)+(int(m)*60)+int(s)
    print second
    return second

def clearCG():
    global sock, cgupdate
    if cgupdate == 1:
        cmd = 'cg 1 clear 10\r\n'
        SendToServer(sock, cmd)
        cgupdate = 0
        
def cgSendCmd(commercial):
    global cgupdate
    
    str = "CG 1 ADD 10 \"AKCESS_CG/Templates/AKCESS\" 1 "
    str = str + "\"<templateData>"
    str = str + "<componentData id=\\"
    str = str + "\"LoadXMLFile\\\">"
    if commercial == 0:
        str = str + "<data id=\\" + "\"text\\\"" +  " value=\\" + "\"D:\\\Intro.axd\\\" />" + "</componentData>"        
    if commercial == 1:
        str = str + "<data id=\\" + "\"text\\\"" +  " value=\\" + "\"D:\\\Commercial.axd\\\" />" + "</componentData>"
    str = str + "<componentData id=\\"
    str = str + "\"LoadLogo\\\">"
    str = str + "<data id=\\" + "\"text\\\"" + " value=\\" + "\"D:/caspar.jpg\\\" />" + "</componentData>"

    str = str + "<componentData id=\\"
    str = str + "\"LoadBG_Strip\\\">"
    str = str + "<data id=\\" + "\"text\\\"" + " value=\\" + "\"D:/BG_Strip.png\\\" />" + "</componentData>"

    str = str + "<componentData id=\\"
    str = str + "\"SetSpeed\\\">"
    str = str + "<data id=\\" + "\"text\\\"" + " value=\\" + "\"3\\\" />" + "</componentData>"
    str = str + "</templateData>" + "\r\n"
    
    SendToServer(sock, str)
    cgupdate = 1

def isAscii(s):
    for c in s:
        if c not in string.ascii_letters:
            return False
    return True

def PickString(dgram, idx):
    name = ''
    while idx < len(dgram) and (dgram[idx] == ' ' or isAscii(dgram[idx]) == False):
            idx += 1
            
    while idx < len(dgram) and (dgram[idx] != '.' or dgram[idx] != ' ' or dgram[idx] != '\0'):
        name += dgram[idx]
        idx += 1
    return name
## change get_float #########################
def get_float(dgram, start_index):
    """
    Get a 32-bit big-endian IEEE 754 floating point number from the datagram.
    Args:
    dgram: A datagram packet.
    start_index: An index where the float starts in the datagram.
    Returns:
    A tuple containing the float and the new end index.
    Raises:
    ParseError if the datagram could not be parsed.
    """
    try:
        if len(dgram[start_index:]) < 1000:   #here change _FLOAT_DGRAM_LEN to 1000 ###############################
        # Noticed that Reactor doesn't send the last bunch of \x00 needed to make
        # the float representation complete in some cases, thus we pad here to
        # account for that.
            dgram = dgram + b'\x00' * (_FLOAT_DGRAM_LEN - len(dgram[start_index:]))
            #print dgram
            float_time = struct.unpack('>f',dgram[start_index:start_index + _FLOAT_DGRAM_LEN])[0]
            index = start_index + _FLOAT_DGRAM_LEN
            return ( float_time,index)
              #struct.unpack('>f',dgram[start_index:start_index + _FLOAT_DGRAM_LEN])[0])
                      #dgram[start_index:start_index + _FLOAT_DGRAM_LEN])[0],
        #start_index + _FLOAT_DGRAM_LEN)
    except (struct.error, TypeError) as e:
        #raise ParseError('Could not parse datagram %s' % e)
        print e
        pass
        


def BuddyOscMsg(packet, filename):
    global nowplaying, layerpaused
    ispaused = False
    # a Typical pattern is like /channel/1/stage/layer/10/file/time ,ff
    tret = []
    ret = packet.find('10/file/path')
    #print "10/file/path   pattern find at position "+str(ret)
    if ret != -1:
        ret = packet.find(',s', ret)
        #print " 's   pattern find at position "+str(ret)
        if ret == -1:
            return tret
        ret += 4
        tmpplaying = PickString(packet, ret)
        #print "tmpplaying, filename", tmpplaying, filename
        tmpplaying = tmpplaying.lower()
        filename = filename.lower()
        if tmpplaying.find(filename) == -1:
            return tret
        #print "Looking for ", filename
        ret = packet.find('file/time')
        #print "file/time   pattern find at position "+str(ret)
        if ret != -1:
            ret = packet.find(',ff', ret)
            #print ",ff   pattern find at position "+str(ret)
            if ret == -1:
                return tret
            ret += 4
            #print packet[ret:]
            #print "end packet --------"   #no problem arises here 
            fval, index = get_float(packet, ret)
            fval2, newindex = get_float(packet, index)
            tret.append(fval)
            tret.append(fval2)
            #print "%s is played %f of %f" % (tmpplaying, fval, fval2)
            return tret

# format h:m:s '12:12:01'
# ct currenttime, st starttime
def WaitTime(ct, st):
    print ct, st
    try:
        ch,cm,cs = ct.split(':')
    except:
        return
    try:
        sh,sm,ss = st.split(':')
    except:
        return
    
    dh = int(sh) - int(ch)
    dm = int(sm) - int(cm)
    ds = int(ss) - int(cs)
    totals = 0
    if dh > 0:
        totals += dh * 60
    if dm > 0:
        totals += dm * 60
    totals = totals + ds
    print totals
    return totals

def UpdateBuddyDb(name,scheduler_id,state):
    buddydb = MySQLdb.connect(buddyip, buddydbuser, buddydbpass, buddydbname)
    buddycursor = buddydb.cursor()
    sql = "UPDATE playlist_1 SET state='%s' where name='%s' and scheduler_id='%s'" % (state, name, scheduler_id)
    try:
        buddycursor.execute(sql)
        buddydb.commit()
    except:
        print "DB update failed"
    buddydb.close()

'''
    WARNING!!! All the playlist items needs to have an unique name.
    A typical name should contain clipname with name-time-date of playout.
    Example - clip1-14-10-03-06-2015
'''
def UpdateDb(name, scheduler_id, state):
    global isbuddydbrunning
    db = MySQLdb.connect(serverip, serverdbuser, serverdbpass, serverdbname)
    cursor = db.cursor()
    sql = "UPDATE playlist_1 SET state='%s' where name='%s' and scheduler_id='%s'" % (state, name, scheduler_id)
    try:
        cursor.execute(sql)
        db.commit()
        
        sql = ''
        sql = "UPDATE playlist_1 SET pushtime=now() WHERE name='%s' AND scheduler_id='%s'" % (name, scheduler_id)
        cursor.execute(sql)
        db.commit()
    except:
        print "DB update failed"
    db.close()
    '''
    try:
        if isbuddydbrunning == True:
            UpdateBuddyDb(name, id, state)
    except:
        pass
        '''

'''
    OSCGetSleepTime - try to figure out %name's playout time via OSC
    %returns -1 on failure
'''
def OSCGetSleepTime(name):
    global oscsock
    count = 10
    #print "Into OSCGetSleepTime"
    # print count #########################################################################################################
    while True:
       
        #print "after count 10-----------------------"
        data, addr = oscsock.recvfrom(512)
        #print "address "
        #print addr
        
        if addr == buddyip:
            continue
        #print name 
        #print data  ## no problem here ----------------------------------
        timeval = BuddyOscMsg(data, name)
        #print "timeval", timeval
        if timeval == None:
            continue
        try:
            if len(timeval) == 0:
                continue
            #print "%s is off length %f" % (name, timeval[1])
            #count -= 1
            diff = timeval[1] - timeval[0]
            return diff
        except:
            return -1.0

def PrepareScroller():
    global commercialtimelist
    try:
        tym = commercialtimelist.pop(len(commercialtimelist)-1).split('|')
    except:
        tym = []
        pass
    msg1 = "<?xml version=\"1.0\" encoding=\"utf-16\" standalone=\"yes\"?>"
    msg2 = "<NewScrollData> <ScrollData> <Story>"
    msg3 = "ফিরছি %s সেকেন্ড পর, আমাদের সথেই থাকুন." % tym[1]
    #msg3 = "We will be back after %s seconds, Stay With Us." % tym[1]
    msg4 = "</Story> </ScrollData> </NewScrollData>"
    msg = msg1 + msg2 + msg3 + msg4
    f = open("D:\\Commercial.axd","w")
    f.write(msg)
    f.close()

def playoutHandler(val):
    global scriptisrunning, isRecovery, sock, logooff, cgrunning, isbuddyplayingcommercial, osct1, osct2, commercialtimelist,liveStarted
    prevclip = ''
    totalFrame = 0
    global schedulerID, buddyvideotimecode, getLiveExitCommand
    try:
        sock = CreateClient()
    except:
        print "Failed to Connect To CasparCG Server, Make Sure Server is Running"
        return
    
    SendToServer(sock, "version\r\n")
    data = sock.recv(20)
    #print data
    sleeptime = 0
    
    ''' The following is for recovery, ie. catching up with where buddy is running'''
    if val > 0.0:
        isRecovery = True
        totalTime = 0
        st = val[0] - int(val[0])
        st = st/.04
        print "seek into frame", (val[1] - val[0])
        osct2 = time.time()
        td = (osct2 - osct1)/.04
        seekt = (int(val[0]) * 25) + st + td + 12
        
        hour, min, sec, msec = buddyvideotimecode.split(":")
        totalTime = float((int(hour)*60*60) + (int(min)*60) + int(sec) + float(int(msec)/25))
        print buddyvideotimecode
        print totalTime
        sleeptime = totalTime - float(seekt/25)
        print sleeptime
        
        
        pcmd = "loadbg 1-10 %s seek %d auto\r\n" % (buddynowplaying, int(seekt))
        ####################
        #add 14-10-2015
        print pcmd
        ####################
        SendToServer(sock, pcmd)
       
        time.sleep(sleeptime)
        
        db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
        cursor = db.cursor()
        sql = "UPDATE playlist_1 SET state='2' where name='%s' and state = '1'" % (buddynowplaying)
        results = ''
        try:
            cursor.execute(sql)
            db.commit()
        except:
            pass
        db.close()
        # now read data from the file and starts playout
        # loadbg 1-10 tomnje seek 251 length 751 auto
        # name|id|timecode|starttime|seek|length
        # TODO: Handle commercial, put commercial and programs on different layers
        # needs to handle properly

    playlistlen = len(myplaylist)
    
    while True:
        try:
            currlist = myplaylist.pop(0)
        except:
            
            FetchPlayList(1)
            if len(myplaylist) == 0:
                print str(datetime.datetime.now())+" Nothing to be played, retrying within 5 seconds..."
                time.sleep(0.5)
            
            continue
        #name, id, tlength, starttime, seek, length, commercial = myplaylist[i].split("|")
        name, scheduler_id, tlength, starttime, seek, length, label = currlist.split("|")
        hour, min, sec, msec = tlength.split(":")
        sleeptime = float(hour)*60*60 + float(min)*60 + float(sec) + float(msec)/25
        totalFrame = int(sleeptime * 25)
        timeDiff = ''
        timeDiff = datetime.datetime.now() - datetime.datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S")
        pcmd = ''
        nowTime = ''
        if str(timeDiff) > "0:30:0.000000":
            UpdateDb(name, scheduler_id, '2')
        elif label[:4].upper() == 'LIVE':
            getLiveExitCommand = ''
            if sleeptime > 0:
                if label.upper() == 'LIVE_1':
                    pcmd = "remove 1 decklink 1\r\n"
                    liveStarted = 1
                    SendToServer(sock, pcmd)
                    print pcmd
                    pcmd = "play 1-10 decklink device 1 format 1080i5000 mix 10\r\n"
                elif label.upper() == 'LIVE_2':
                    pcmd = "remove 1 decklink 2\r\n"
                    SendToServer(sock, pcmd)
                    liveStarted = 2
                    print pcmd
                    pcmd = "play 1-10 decklink device 2 format 1080i5000 mix 10\r\n"
                elif label.upper() == 'LIVE_3':
                    pcmd = "remove 1 decklink 3\r\n"
                    SendToServer(sock, pcmd)
                    liveStarted = 3  
                    print pcmd
                    pcmd = "play 1-10 decklink device 3 format 1080i5000 mix 10\r\n"
                else:
                    pcmd = "remove 1 decklink 4\r\n"
                    SendToServer(sock, pcmd)
                    liveStarted = 4
                    print pcmd
                    pcmd = "play 1-10 decklink device 4 format 1080i5000 mix 10\r\n"
                SendToServer(sock, pcmd)

                nowTime = str(datetime.datetime.now())
                print nowTime +" "+pcmd
                UpdateDb(name, scheduler_id, '1')
                PlayedLog(scheduler_id,label,name,tlength,nowTime)
                while 1:
                    if not getLiveExitCommand: continue
                    getLiveExitCommand = ''
                    break
            UpdateDb(name, scheduler_id, '2')
        else:
    
            pcmd = 'play 1-10 "%s" length %s auto\r\n' % (name,str(totalFrame))
            SendToServer(sock, pcmd)
            nowTime = str(datetime.datetime.now())
            print nowTime +" "+pcmd
            UpdateDb(name, scheduler_id, '1')
            #===================Decklink Add with embedded audio===========
            if liveStarted > 0:
                '''
                if liveStarted == 1:
                    pcmd = "add 1 decklink 1 embedded_audio\r\n"
                elif liveStarted == 2:
                    pcmd = "add 1 decklink 2 embedded_audio\r\n"
                elif liveStarted == 3:
                    pcmd = "add 1 decklink 3 embedded_audio\r\n"
                elif liveStarted == 4:
                    pcmd = "add 1 decklink 4 embedded_audio\r\n"
                    
                print nowTime +" "+pcmd
                SendToServer(sock, pcmd)
                '''
                liveStarted = 0
            #==============================================================
        
            
            PlayedLog(scheduler_id,label,name,str(totalFrame),nowTime)
            waitForNext(sleeptime - 0.036) 
            UpdateDb(name, scheduler_id, '2')
            
            # myplaylist.pop(i)
        
            
    sock.close()

'''
    * GetBuddyIP, db, credentials
    * make connection
    * fetch list from db => id,name,starttime
    * buddyplaylist[], keep all the program list in it
    TODO: Also make sure that this DB and buddy DB are sync.
'''
def CheckBuddyPlayList():
    global isbuddydbrunning
    
    try:
        db = MySQLdb.connect(buddyip, buddydbuser, buddydbpass, buddydbname)
        cursor = db.cursor()
        sql = "SELECT name,scheduler_id,timecode,starttime,seek,length,commercial FROM playlist_1 WHERE state='0' limit 1"
        print "connected to buddy db"
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            print "Got Results of len: %d" % len(results)
            #########################################
            isbuddydbrunning = True
            ########################################
            for r in results:
                global myplaylist
                #print r[0], r[1]
                tmp = r[0] +'|'+ str(r[1]) + '|' + str(r[2]) + '|' + str(r[3]) + '|' + str(r[4]) + '|' + str(r[5]) + '|' + str(r[6])
                print tmp
                myplaylist.append(tmp)
        except:
            isbuddydbrunning = False
            print "Failed To FetchPlayList From BUDDY DB"
        print "After myplaylist update length => ", len(myplaylist)
        db.close()
    except:
        isbuddydbrunning = False
        print "Make Sure Buddy DB is on"
        pass
    



'''
Here we'll be checking the buddy's playlist and will compare it with ours
if any mismatch found playlist will be merged
%skipbuddy - indicates whether we should check buddy server
'''
def FetchPlayList(skipbuddy):
    global myplaylist, isbuddydbrunning
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    # TODO: select only name,id,timecode,commercial flags
    sql = "SELECT name,scheduler_id,timecode,starttime,seek,length,label FROM playlist_1 WHERE state='0' order by starttime asc limit 1"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        print "Got Results of len: %d" % len(results)
        for r in results:
            tmp = r[0] + '|' + str(r[1]) + '|' + str(r[2]) + '|' + str(r[3]) + '|' + str(r[4]) + '|' + str(r[5]) + '|' + str(r[6])
            print tmp
            myplaylist.append(tmp)
        # mark all the items as not played
        # for i in results
    except:
        print "Failed To FetchPlayList From DB"
        pass
    db.close()
 

'''
createtable - create new tables required by the script
If already exist, skip silently, for now hardcode the
username,pass,dbname
'''
def createtable():
    print "serverparam %s %s %s %s" % (serverip, serverdbname, serverdbpass, serverdbuser)
    db = MySQLdb.connect(serverip, serverdbuser, serverdbpass, serverdbname)
    cursor = db.cursor()
    sql = "CREATE TABLE IF NOT EXISTS `playlist` (\
  `ID` int(11) NOT NULL AUTO_INCREMENT,\
  `type` varchar(255) DEFAULT NULL,\
  `devicename` varchar(255) DEFAULT NULL,\
  `label` varchar(255) DEFAULT NULL,\
  `name` varchar(255) DEFAULT NULL,\
  `channel` varchar(255) DEFAULT NULL,\
  `videolayer` varchar(255) DEFAULT NULL,\
  `delay` varchar(255) DEFAULT NULL,\
  `allowgpi` varchar(255) DEFAULT NULL,\
  `allowremotetriggering` varchar(255) DEFAULT NULL,\
  `remotetriggerid` varchar(255) DEFAULT NULL,\
  `storyid` varchar(255) DEFAULT NULL,\
  `transition` varchar(255) DEFAULT NULL,\
  `transitionDuration` varchar(255) DEFAULT NULL,\
  `tween` varchar(255) DEFAULT NULL,\
  `direction` varchar(255) DEFAULT NULL,\
  `seek` varchar(255) DEFAULT NULL,\
  `length` varchar(255) DEFAULT NULL,\
  `freezeonload` varchar(255) DEFAULT NULL,\
  `triggeronnext` varchar(255) DEFAULT NULL,\
  `autoplay` varchar(255) DEFAULT NULL,\
  `color` varchar(255) DEFAULT NULL,\
  `timecode` varchar(255) DEFAULT NULL,\
  `starttime` varchar(255) DEFAULT NULL,\
  `state` varchar(255) DEFAULT NULL,\
  `pushtime` datetime(2) DEFAULT NULL,\
  `duration` varchar(255) DEFAULT NULL,\
  `useauto` varchar(255) DEFAULT NULL,\
  `commercial` varchar(1) DEFAULT '0',\
  `asset_id` varchar(255) DEFAULT NULL,\
  `scheduler_id` int(11) NOT NULL,\
  `createdby` varchar(255) NOT NULL,\
  `created` datetime NOT NULL,\
  `updated` datetime DEFAULT NULL,\
  `updatedby` varchar(255) DEFAULT NULL,\
  PRIMARY KEY (`ID`)\
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=10127 ;"
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
        print "Table already created"
    db.close()
    
'''
Uncomment later on
'''
def createDB():
    db = MySQLdb.connect(serverip, serverdbuser, serverdbpass, serverdbname)
    cursor = db.cursor()
    sql = 'CREATE database casparcg'
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
        print "DB already created"
    db.close()

def initdb():
    createtable()

def CreateConFile():
    try:
        p = os.environ.get('LOCALAPPDATA')
        print p
        p = p + "\\CSClient"
        try:
            os.mkdir(p)
            p = p + "\\csclient.ini"
            cmd = "copy nul %s > nul" % p
            try:
                os.system(cmd)
            except:
                print "failed to create ini file"
        except:
            print "Failed to create directory"
    except:
        print "Failed to create dir"

def ReadConFile():
    fpath = os.environ.get('LOCALAPPDATA')
    p = fpath + "\\CSClient\\csclient.ini"
    try:
        confile = open(p, 'r')
        #print confile
        confile.seek(0)
        while True:
            ret = confile.readline()
            #print ret
            if ret == '':
                break
            if ret == '[General]':
                continue
            array = ret.split('=')
            if array[0] == 'buddyip':
                global buddyip
                buddyip = array[1].rstrip('\n')
                continue
            if array[0] == 'buddydbname':
                global buddydbname
                buddydbname = array[1].rstrip('\n')
                continue
            if array[0] == 'buddydbuser':
                global buddydbuser
                buddydbuser = array[1].rstrip('\n')
                continue
            if array[0] == 'buddydbpass':
                global buddydbpass
                buddydbpass = array[1].rstrip('\n')
                continue
            if array[0] == 'serverip':
                global serverip
                #array = ret.split('=')
                serverip = array[1].rstrip('\n')
                continue
            if array[0] == 'serverdbname':
                global serverdbname
                serverdbname = array[1].rstrip('\n')
                continue
            if array[0] == 'serverdbpass':
                global serverdbpass
                serverdbpass = array[1].rstrip('\n')
                continue
            if array[0] == 'serverdbuser':
                global serverdbuser
                serverdbuser = array[1].rstrip('\n')
                continue
            if array[0] == 'servername':
                global servername
                servername = array[1].rstrip('\n')
                continue
            if array[0] == 'logpath':
                global logpath
                logpath = array[1].rstrip('\n')
                print logpath
        confile.close()
    except:
        print "Failed to read confile"
        confile.close()

def initCSClient():
    CreateConFile()
    ReadConFile()

def GetBuddyNowPlaying():   ### change serverip to buddyip
    db = MySQLdb.connect(buddyip,buddydbuser,buddydbpass,buddydbname)
    cursor = db.cursor()
    sql = "SELECT scheduler_id,name,timecode FROM playlist_1 WHERE state='1'"
    results = ''
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        pass
    db.close()
    if results != '':
        for r in results:
            tmp = str(r[0]) + '|' + str(r[1]) + '|' + str(r[2])
            return tmp
    else:
        return results
    


def CreateOscSocket():
    global oscsock
    oscsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    oscsock.bind(('', OscClientPort))

'''
This function basically detects whether we're on recovery phase or initializing for the first time.
A tentative technique is to listen for OSC messages, try to listen for few seconds to get an idea of
when to trigger.
'''
def isRecoveryStartup():
    global buddynowplaying, oscsock, isbuddyplayingcommercial, isbuddydbrunning
    global schedulerID, buddyvideotimecode
    CreateOscSocket()
    if isbuddydbrunning == False:
        return 0.0
    tmp = GetBuddyNowPlaying()
    #print "*************************", buddynowplaying
    if len(tmp) == 0:
        return 0.0
    buddynowplaying = tmp
    if buddynowplaying == '':
        print "*******************Not Recovery Startup"
        return 0.0
    print "=========>>>buddy is playing", buddynowplaying
    print "Waiting for OSC MSG"
    schedulerID, buddynowplaying, buddyvideotimecode = buddynowplaying.split('|')
    count = 10
    while count > 0:
        data, addr = oscsock.recvfrom(512)
        #print addr, data
        if addr == serverip:
            continue
        # take only buddyOSC messages
        timeval = BuddyOscMsg(data, buddynowplaying)
        try:
            if len(timeval) == 0:
                continue
            print "Played %f of %f seconds" % (timeval[0], timeval[1])
            count -= 1
            diff = float(timeval[1]) - float(timeval[0])
            return timeval
        except:
            pass
        if count == 0:
            return 0.0

def CGWriteToFile(news):
    global sock, cgupdate, cgrunning
    '''CG 1 ADD 10 "AKCESS_CG/Templates/AKCESS" 1 "<templateData><componentData id=\"LoadXMLFile\"><data id=\"text\" value=\"D:\\Intro.axd\" /></componentData><componentData id=\"LoadLogo\"><data id=\"text\" value=\"C:/caspar.jpg\" /></componentData><componentData id=\"LoadBG_Strip\"><data id=\"text\" value=\"C:/BG_Strip.png\" /></componentData><componentData id=\"SetSpeed\"><data id=\"text\" value=\"5\" /></componentData><componentData id=\"SetFontSize\"><data id=\"text\" value=\"50\" /></componentData></templateData>\r\n
'''
    #if cgupdate == 0:
    #    str = "CG 1 ADD 10 \"AKCESS_CG/Templates/AKCESS\" 1 "
    #else:
    if cgupdate == 1:
        SendToServer(sock, "cg 1 remove 10\r\n")

    f = open("D:\\Intro.axd","w")
    f.write(news)
    f.close()
    cgSendCmd(0)
    cgrunning = True
    
def putSquzee():
    global sock
    SendToServer(sock, "MIXER 1-10 FILL .15 0 .85 .80\r\n")

'''
    %CG_Handler - this thread is dedicated for handling request from client.
    This will use the internally deployed flash script and will put text on
    scroll or other various stuffs.
'''
def CG_Handler():
    global cgrunning
    cgsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cgsock.setblocking(0)
    cgsock.bind(('', 8000))
    input = [cgsock]
    while True:
        inputready,outputready,exceptready = select.select(input,[],[])
        for s in inputready:
            if s == cgsock:
                cgdata, addr = cgsock.recvfrom(65565)
                if len(cgdata) == 6:
                    if cgdata == 'squzee':
                        putSquzee()
                        continue
                if len(cgdata) == 7:
                    if cgdata == 'clearcg':
                        cgrunning = False
                        clearCG()
                        continue
                #print "Data %s recvfrom %s" % (cgdata,addr)
                CGWriteToFile(cgdata)

# Update own server playlist state, when we are sure that we are
# on recovery state, that means we could be crashed in the middle of
# something, so make sure we don't have anything at state='1'
def UpdateMyDbState():
    db = MySQLdb.connect(buddyip,buddydbuser,buddydbpass,buddydbname)
    cursor = db.cursor()
    
    sql = "select scheduler_id from playlist_1 where state = 1"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            BuddyNowPlayingID = int(r[0])
            print BuddyNowPlayingID
    except:
        
        pass
    db.close()
    
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    sql = "UPDATE playlist_1 SET state='2' WHERE scheduler_id = %s" % (str(BuddyNowPlayingID))
    results = ''
    try:
        cursor.execute(sql)
        db.commit()
    except:
        pass
    sql = "UPDATE playlist_1 SET state='2' WHERE starttime <= '%s'" % (str(datetime.datetime.now()))
    results = ''
    try:
        cursor.execute(sql)
        db.commit()
    except:
        pass
    db.close()
   

def GenerateFilename():
    prefix='caspar_'
    t = time.localtime()
    p = str(t.tm_year)
    if len(str(t.tm_mon)) > 1:
        p = p + '-' + str(t.tm_mon)
    else:
        p = p + '-0' + str(t.tm_mon)
    if len(str(t.tm_mday)) > 1:
        p = p + '-' + str(t.tm_mday)
    else:
        p = p + '-0' + str(t.tm_mday)
    prefix += p + ".log"
    print "GenerateFilename ==>", prefix
    return prefix

def parseMessage(logf):
    buf = []
    try:
        f = open(logf, 'r')
    except:
        print "Unable to open file. Makesure filename is okay"
        return buf
    while True:
        t = f.readline()
        if t == '':
            break
        m = t.find('transition[empty=>ffmpeg[')
        if m > 0:
            m += 25
            m = t.find('Uninitialized.', m)
            if m > 0:
                buf.append(t)
                continue
        m = t.find(' transition[ffmpeg[')
        if m > 0:
            m += 18
            m = t.find('Uninitialized.', m)
            if m > 0:
                buf.append(t)
                continue
    f.close()
    return buf

def GenerateReport():
    global logpath
    preparebuf = []
    path = logpath #"D:\CasparCG\caspercg server\Server\log"
    #path = "log"
    fname = GenerateFilename()
    path = path + '\\' + fname
    print "Invoking Generate Report"
    rbuf = parseMessage(path)
    if len(rbuf) == 0:
        print "Nothing to Parse"
        return preparebuf

    for i in range(0,len(rbuf)):
        l = rbuf[i].split(' ')
        line = l[1].strip(']') + '|' + l[7].split('=>')[1].strip('ffmpeg').strip('[').split('|')[0]+'\r\n'
        preparebuf.append(line)
        print "%s added" % (line)
    return preparebuf

def ReportHandler():
    reportbuf = []
    reportsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    reportsock.setblocking(0)
    reportsock.bind(('', 9000))
    reportsock.listen(5)
    input = [reportsock]
    output = [reportsock]
    print "ReportHandler Ready..."
    
    while True:
        inputready,outputready,exceptready = select.select(input,output,[])
        for s in inputready:
            if s == reportsock:
                connection, client_addr = s.accept()
                connection.setblocking(0)
                input.append(connection)
            else:
                reportcmd = s.recv(1024)
                if reportcmd.find("GETREPORT") != -1:
                    reportbuf = GenerateReport()
                    if len(reportbuf) > 0:
                        sizeb = len(reportbuf)
                        for i in range(0,sizeb):
                            item = reportbuf.pop()
                            # we'll be blocking here, nevermind; we dont care. we'll be done when it's finished
                            s.send(item)
                            print "sending to client", item
                else:
                    # We are done now, we're not supposed to get any commands
                    s.close()
                    input.remove(s)
                    
def checkBuddyDbRunning():
    global isbuddydbrunning
    db = MySQLdb.connect(buddyip,buddydbuser,buddydbpass,buddydbname)
    cursor = db.cursor()
    sql = "SELECT count(name) FROM playlist_1 WHERE state='1'"
    results = ''
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            tmp = int(r[0])
        if tmp == 1:
            isbuddydbrunning = True
        else:
            isbuddydbrunning = False
    except:
        pass
    db.close()
def CheckOwnDatabaseEmpty():
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    
    sql = "select count(name) from playlist_1"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            tmp = int(r[0])
        if tmp > 0:
            return 1 
        else:
            return 0
    except:
        return 0
        pass
    db.close() 
    
def CheckBuddyDatabaseEmpty():
    db = MySQLdb.connect(buddyip,buddydbuser,buddydbpass,buddydbname)
    cursor = db.cursor()
    
    sql = "select count(name) from playlist_1"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            tmp = int(r[0])
        if tmp > 0:
            return 1 
        else:
            return 0
    except:
        return 0
        pass
    db.close() 
    
def CopyBuddyDatabase():
    db = MySQLdb.connect(buddyip,buddydbuser,buddydbpass,buddydbname)
    cursor = db.cursor()
    sql = "select ID,label,name\
        ,timecode,starttime,seek,length\
        ,state,pushtime,asset_id\
        ,scheduler_id,createdby,created,updated,updatedby from playlist_1"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
   
    except:
        print "error"
        pass
    db.close()

    result_list = []
    for result in results:
        result_list.append(result)


    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    #"UPDATE playlist SET state='%s' where name='%s'" % (state, name)
    sql = "insert into playlist_1 (ID,label,name\
    ,timecode,starttime,seek,length\
    ,state,pushtime\
    ,asset_id,scheduler_id,createdby,created,updated,updatedby)\
    values\
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,\
    %s,%s,%s,%s,%s,%s)"
    try:
        cursor.executemany(sql,result_list)
        db.commit()
    except:
        db.rollback()
        print "DB already created"
    db.close()

def CheckOwnAndBuddyDatabaseEquality():
    tmp_server = 0
    tmp_buddy = 0
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    sql = "SELECT count(name) FROM playlist_1"
    results = ''
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            tmp_server = int(r[0])
            #print tmp_server
    except:
        pass
    db.close()
    db = MySQLdb.connect(buddyip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    sql = "SELECT count(name) FROM playlist_1"
    results = ''
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            tmp_buddy = int(r[0])
            #print tmp_buddy
    except:
        pass
    db.close()
    if tmp_server < tmp_buddy:
        return False
    else :
        return True

def DeleteOwnDatabase():
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    sql = "delete from playlist_1"
    
    try:
        cursor.execute(sql)
        db.commit()
    except:
        pass
    db.close()

def UpdateOwnDbStateIfBuddyPlayedAll():
    tmp_buddy = 0
    db = MySQLdb.connect(buddyip,buddydbuser,buddydbpass,buddydbname)
    cursor = db.cursor()
    sql = "SELECT count(id) FROM playlist_1 WHERE state='0'"
    results = ''
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            tmp_buddy = int(r[0])
    except:
        pass
    db.close()
    if tmp_buddy == 0:
        db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
        cursor = db.cursor()
        for r in results:
            sql = "update playlist_1 set state = 2 where state in (0,1)"

            try:
                cursor.execute(sql)
                db.commit()
            except:
                pass
        db.close()

def serverCaspar():
    global getLiveExitCommand 
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 50007))
    while True:
        s.listen(1)
        conn, addr = s.accept()
        data = conn.recv(1024)
        if not data: continue
        getLiveExitCommand = data
    conn.close()
        
def PlayedLog(scheduler_id,label,name,duration,played_time):
    try:
        db = MySQLdb.connect('10.3.10.197','root','password','project')
        cursor = db.cursor()
        if servername == 'primary':
            sql = "insert into primary_log (scheduler_id,program_type,name,duration,played_time)\
            values (%d,'%s','%s','%s','%s')" % (int(scheduler_id),label,name,duration,played_time)
        elif servername == 'secondary':
            sql = "insert into secondary_log (scheduler_id,program_type,name,duration,played_time)\
            values (%d,'%s','%s','%s','%s')" % (int(scheduler_id),label,name,duration,played_time)
    
        cursor.execute(sql)
        db.commit()
    except:
        AlternativeLog(scheduler_id,label,name,duration,played_time)
    else:
        db.close()

def AlternativeLog(scheduler_id,label,name,duration,played_time):
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

def waitForNext(waitTimeForNext):
    global getLiveExitCommand
    getLiveExitCommand = ''
    start_time = time.time()
    while waitTimeForNext > 0:
        end_time = time.time()
        waitTimeForNext = waitTimeForNext - (end_time - start_time)
        start_time = end_time
        if not getLiveExitCommand: continue
        getLiveExitCommand = ''
        break
    
        
if __name__ == "__main__":

    ##############################
    #create and save general and error log file
    starting_log_date = time.strftime("%Y-%m-%d")
    starting_log_time = time.strftime("%H_%M_%S")
    general_log = "log\\" + starting_log_date +"_"+starting_log_time+"-Casparcg_Python_Script_log.txt"
    error_log = "log\\" + starting_log_date +"_"+starting_log_time+"-Casparcg_Python_Script_Error.txt"
    sys.stdout = Logger(general_log)

    #generate error log file
    sys.stderr = open(error_log, 'a')

    ##################################
    #global osct1
    initCSClient()
    initdb()
    '''
    if CheckOwnDatabaseEmpty()==0 and CheckBuddyDatabaseEmpty()==1:
        #print "own database empty"
        CopyBuddyDatabase()
    '''   
    #FetchPlayList(0)
    print "server running =",servername 
    osct1 = time.time()
    checkBuddyDbRunning()  #changed , before it has no existance
    ############################################
    if CheckOwnAndBuddyDatabaseEquality() == False :
        DeleteOwnDatabase()
        CopyBuddyDatabase()
    ############################################   
    ret = isRecoveryStartup()  
    #############################################################
    # print ret 
    #print " is recovery start up return val RET -------------- > "+str(ret)  ##### OK,No problem here 
    if ret != 0.0:
        UpdateMyDbState()
    UpdateOwnDbStateIfBuddyPlayedAll()
    FetchPlayList(0)
    #CommercialTimes()
    thread.start_new_thread(CG_Handler,())
    thread.start_new_thread(ReportHandler,())
    thread.start_new_thread(serverCaspar,())
    playoutHandler(ret)

    sys.stderr = sys.__stderr__
    sys.stderr.close()
    
