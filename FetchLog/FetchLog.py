'''
python script for sync playout logs between playout server and razuna server
written by : Md.Rifat-Ut-Tauwab
'''



import socket, os, string
import MySQLdb, select, sys
import thread, time, struct, decimal
import datetime,logging

primaryServer = '10.10.10.11'
secondaryServer = '10.10.10.12'
razunaServer = '10.10.10.13'
serverdbrundown = 'casparcg'
serverdbrazuna = 'project'
serverdbpass = 'password'
serverdbuser = 'root'


def GetMaxPlayedTime(server_log):
    temp = ''
    sql = "SELECT max(played_time) as played_time FROM %s" % (server_log)
    try:
        db = MySQLdb.connect(razunaServer,serverdbuser,serverdbpass,serverdbrazuna)
        cursor = db.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            temp = r[0]
    except:
        message = str(datetime.datetime.now())+" -> Failed to get max played time for %s" % (server_log)
        print message
        logging.basicConfig(filename= os.getcwd()+"/log/"+time.strftime("%Y-%m-%d")+'_FetchLog.log',level=logging.INFO)
        logging.warning(message)
    else:
        db.close()
    return temp

def GetNewLogRecord(serverIp,server_log):
    myLogList = []
    maxDate = GetMaxPlayedTime(server_log);
    if maxDate == None:
        sql = "SELECT * FROM alternative_log"
    else:
        sql = "SELECT * FROM alternative_log where played_time > '%s' order by played_time" % (maxDate)
    #print sql
    try:
        db = MySQLdb.connect(serverIp,serverdbuser,serverdbpass,serverdbrundown)
        cursor = db.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        for r in results:
            temp = str(r[0])+ '|' + str(r[1]) + '|' + str(r[2]) + '|' + str(r[3]) + '|' + str(r[4])
            myLogList.append(temp)
    except:
        message = str(datetime.datetime.now())+" -> Failed to get New log Record for %s"%(serverIp)
        print message
        logging.basicConfig(filename= os.getcwd()+"/log/"+time.strftime("%Y-%m-%d")+'_FetchLog.log',level=logging.INFO)
        logging.warning(message)
    else:
        db.close()
    return myLogList

def CreateLogtoRazuna(serverIp,server_log):
    myLogList = []
    myLogList = GetNewLogRecord(serverIp,server_log)
    message = str(datetime.datetime.now())+"  -> Fetch data Starts for %s...." % (serverIp)
    print message
    logging.basicConfig(filename= os.getcwd()+"/log/"+time.strftime("%Y-%m-%d")+'_FetchLog.log',level=logging.INFO)
    logging.info(message)
    for log in myLogList:
        scheduler_id,pgm_type,name,duration,played_time = log.split("|")
        InsertDataSecondary(server_log,scheduler_id,pgm_type,name,duration,played_time)
    message = str(datetime.datetime.now())+"  -> Fetching Data ended for %s" % (serverIp)
    print message
    logging.info(message)

def InsertDataSecondary(server_log,scheduler_id,pgm_type,name,duration,played_time):
    
    sql = "insert into %s (scheduler_id,program_type,name,duration,played_time)\
           values (%d,'%s','%s','%s','%s')" % (server_log,int(scheduler_id),pgm_type,name,duration,played_time)
    
    try:
        db = MySQLdb.connect(razunaServer,serverdbuser,serverdbpass,serverdbrazuna)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
    except:
        message = str(datetime.datetime.now())+" -> Insertion failed for %s"%(server_log)
        print message
        logging.basicConfig(filename= os.getcwd()+"/log/"+time.strftime("%Y-%m-%d")+'_FetchLog.log',level=logging.INFO)
        logging.warning(message)
        pass
    else:
        db.close()
    

if __name__ == "__main__":
    #print GetMaxPlayedTime();
    #myLogList = []
    #myLogList = GetNewLogRecord()
    #print myLogList
    logging.basicConfig(filename= os.getcwd()+"/log/"+time.strftime("%Y-%m-%d")+'_FetchLog.log',level=logging.INFO)
    logging.info(str(datetime.datetime.now())+" -> Fetch log script started")
    while True:
        CreateLogtoRazuna(primaryServer,'primary_log')
        CreateLogtoRazuna(secondaryServer,'secondary_log')
        message = str(datetime.datetime.now())+" -> waiting for next log update"
        print message
        logging.info(message)
        time.sleep(60)
