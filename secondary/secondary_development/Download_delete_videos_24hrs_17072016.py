#-*- coding: UTF-8 -*-
'''
DownloadVideoFromRazuna.py - Written by Md.Rifat-Ut-Tauwab<mdruts@gmail.com>,
Sysnova Information Sys Ltd.
This program is written to download video file from razuna Media Asset Manager.
mysql db used - MySQL-python-1.2.4b4.win32-py2.7
taken from http://sourceforge.net/projects/mysql-python
'''


import MySQLdb,os,thread
import urllib,time
from datetime import datetime
import datetime


serverip = '10.3.10.191'
serverdbname = 'casparcg'
serverdbuser = 'root'
serverdbpass = 'password'


def GetMediaFileNameFromDb():
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    sql = "SELECT distinct name FROM playlist_1 where state = '9' or state = '0' order by starttime"
    results = ''
    result = []
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for fileName in results:
            result.append(fileName[0].lower())
            #print fileName[0]
    except:
        pass
    db.close()
    return result

def GetMediaFileName():
    Files = os.listdir("E:\CasparCG Server 2.0.7\CasparCG Server\Server\media")
    mediaFiles = []
    for name in Files:
        #name = name.split('.')
        
        #mediaFiles.append(name[0].lower())
        mediaFiles.append(name.lower())
        #print name.lower()
        
    return mediaFiles


def GetMediaNotFound():
    mediaInDatabase = GetMediaFileNameFromDb()
    #print mediaInDatabase
    mediaInFolder = GetMediaFileName()
    
    mediaNotFound = []
    for fileName in mediaInDatabase:
        try:
            index = mediaInFolder.index(fileName)
        except:
            mediaNotFound.append(fileName)
            
    return mediaNotFound
        
def GetAssetID():
    mediaNotFound = GetMediaNotFound()
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    assetID = []
    for fileName in mediaNotFound:
        
        sql = "SELECT distinct asset_id FROM playlist_1 where name='%s'" % (fileName)
        results = ''
        
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for asset_id in results:
                if asset_id[0] == '':
                    break
                else:
                    assetID.append(asset_id[0])
                    print asset_id[0]
        except:
            pass
    db.close()
    return assetID

def BuildSrcDest():
    assetID = GetAssetID()
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    for asset_id in assetID:
        sql = "SELECT distinct name FROM playlist_1 where asset_id='%s'" % (asset_id)
        results = ''
        
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for name in results:
                asset_name = name[0]
        except:
            pass
        build_url_src = "http://10.3.10.197:8080//razuna/raz2/dam/index.cfm?fa=c.serve_file&file_id=%s&type=vid"%(asset_id)
        
        build_url_dest = "E:\CasparCG Server 2.0.7\CasparCG Server\Server\media\%s"%(asset_name)
        print "%s downloading ..."%(asset_name)
        DownloadMedia(build_url_src,build_url_dest)
        print "%s download completed"%(asset_name)
    db.close()
       
        
def DownloadMedia(src,dest):
    Downloadfile = urllib.URLopener()
    Downloadfile.retrieve(src,dest)

def ChangeStateToZero():   
    db = MySQLdb.connect(serverip,serverdbuser,serverdbpass,serverdbname)
    cursor = db.cursor()
    sql = "update playlist_1 set state = '0' where state = '9'";
    try:
        cursor.execute(sql)
        db.commit()
    except:
        pass
    db.close()
    
    
def StartPlayout():
    file = open('time.vbs')
    for getTime in file:
        str = getTime
    while True:
        if datetime.now().strftime('%Y-%m-%d %H:%M:%S') >= str:
            ChangeStateToZero()
            time.sleep(3)

'''
here code starts for delete programs only
'''
def GetProgramFileName():
    Files = os.listdir("E:\CasparCG Server 2.0.7\CasparCG Server\Server\media")
    mediaFiles = []
    for name in Files:
        if name[:3].lower() == 'pgm' or name[:3].lower() == 'nca':
            mediaFiles.append(name.lower())   
    return mediaFiles

def created_date(filename):
    t = os.path.getctime(filename)
    return datetime.datetime.fromtimestamp(t)

def DeleteFile():
    while True:
        AllMedia = GetProgramFileName()
        for media in AllMedia:
            creation_date = created_date('E:\CasparCG Server 2.0.7\CasparCG Server\Server\media\%s' %(media))
            if creation_date < (datetime.datetime.now()+ datetime.timedelta(days=-7)):
                os.remove('E:\CasparCG Server 2.0.7\CasparCG Server\Server\media\%s' %(media))
        time.sleep(300)
if __name__ == "__main__":
    #BuildSrcDest()
    #ChangeStateToZero()
    #print GetMediaFileNameFromDb()
    #print GetMediaFileName()
    #print GetMediaNotFound()
    thread.start_new_thread(DeleteFile,())
    while True:
        BuildSrcDest()
        time.sleep(5)

    
    
   
    
