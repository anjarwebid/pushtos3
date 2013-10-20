
import sys, os, time, ConfigParser, tarfile, ftplib, datetime
from boto.s3.connection import S3Connection
from boto.s3.key import Key

#global variable here
appname = "pushtos3"
appfolder = os.path.abspath(__file__).strip(appname+".py")
dbserver = ""
dbusr = ""
dbpasswd = ""
pathtobackup = ""
akey = ""
skey = ""
patharray = []
bucket = ''
filestamp = time.strftime('%Y-%m-%d')

def chkcfg():
    global appfolder
    global dbserver
    global dbusr
    global dbpasswd
    global pathtobackup
    global akey
    global skey
    global patharray
    global bucket
    
    if not os.path.isfile(os.path.abspath(__file__).strip(appname+".py")+"%s" % (appname)+".conf"):
        print "config file %s.cfg not found!" % (appname)
        print os.path.abspath(__file__).strip(appname+".py")
    else:
        #if config file exist, just read config file
        readcfg = ConfigParser.ConfigParser()	
        readcfg.read(os.path.abspath(__file__).strip(appname+".py")+"%s" % (appname)+".cfg")
        dbserver = readcfg.get('basic', 'dbserver')
        dbusr = readcfg.get('basic', 'dbuser')
        dbpasswd = readcfg.get('basic', 'dbpassword')
        pathtobackup = readcfg.get('basic','pathtobackup')
        akey = readcfg.get('basic', 'aws_access_key_id')
        skey = readcfg.get('basic','aws_secret_access_key')
        bucket = readcfg.get('basic','bucket')
        patharray = pathtobackup.split(',')
        backupdb()
        #cektanggal()

def backupdb():
    #check and create log
    f = open("/var/log/fullbackups3", "w") 
    f.write ("backup on "+filestamp+"\n")
    # get database list
    database_list_command="mysql -u %s -p%s -h %s --silent -N -e 'show databases'" % (dbusr, dbpasswd, dbserver)
    #split database list
    for database in os.popen(database_list_command).readlines():
        database = database.strip()
        #skip "information_scema" database backup
        if database == 'information_schema':
            continue
        #set folder for database backup files
        newpath = "%s%s/database" % (appfolder,filestamp)
        if not os.path.exists(newpath): 
            os.makedirs(newpath)
        filename = "%s/%s-%s.sql" % (newpath, database, filestamp)
        #generate backup file
        os.popen("mysqldump -u %s -p%s -h %s -e --opt -c %s --skip-lock-tables | gzip -c > %s.gz" % (dbusr, dbpasswd, dbserver, database, filename))
        f.write ("Backup database "+database+" finished\n")
    f.write ("Database Backup finished\n")
    f.close
    backupfile()

def backupfile():
    f = open("/var/log/fullbackups3", "w") 
    for x in patharray:
        lastfolder = os.path.basename(os.path.normpath(x))
        newpath = "%s%s/file" % (appfolder,filestamp)
        if not os.path.exists(newpath):
            os.makedirs(newpath)
        tar = tarfile.open(os.path.join(newpath, lastfolder+'.tar.gz'), 'w:gz')
        tar.add(x,arcname=lastfolder)
        tar.close()
        f.write ("Backup folder "+lastfolder+" finished\n")
    f.write ("All finished\n")
    f.close
    uploadbackup()        

def uploadbackup():
    lastfolder = os.path.basename(os.path.normpath(os.path.join(appfolder, filestamp)))
    tar = tarfile.open(os.path.join(appfolder, filestamp+'.tar.gz'), 'w:gz')
    tar.add(os.path.join(appfolder, filestamp),arcname=lastfolder)
    tar.close()
    
    #backup monthly
    if time.strftime("%d") == "01":
        print "monthly"
        conn = S3Connection(akey, skey)
        s3bucket = conn.get_bucket(bucket)
        k = Key(s3bucket)
        #upload data
        k.key = "monthly/%s.tar.gz" % (filestamp)
        k.set_contents_from_filename("%s" % os.path.join(appfolder, filestamp+'.tar.gz'))
        print "Monthly backup uploaded to s3"
        #delete file backup bulan 
        today = datetime.date.today()
        first = datetime.date(day=1, month=today.month, year=today.year)
        lastMonth = first - datetime.timedelta(days=1)    
        lastmonthfile =  "%s-%s-%s.tar.gz" % (lastMonth.strftime('%Y'), lastMonth.strftime("%m"), time.strftime('%d'))
        k.key = "monthly/%s" % (lastmonthfile)
        if k.exists:
           s3bucket.delete_key(k)

        
    #backup weekly
    if time.strftime("%a") == "Mon": 
        print "Weekly"
        conn = S3Connection(akey, skey)
        s3bucket = conn.get_bucket(bucket)
        k = Key(s3bucket)
        #upload data
        k.key = "weekly/%s.tar.gz" % (filestamp)
        k.set_contents_from_filename("%s" % os.path.join(appfolder, filestamp+'.tar.gz'))
        print "Weekly backup uploaded to s3"
        #delete file last monday dari folder weekly
        today = datetime.date.today()
        lastmonday = today - datetime.timedelta(days=-today.weekday(), weeks=1)
        lastmondayfile = "%s-%s-%s.tar.gz" % (lastmonday.strftime('%Y'), lastmonday.strftime("%m"), lastmonday.strftime('%d'))
        k.key = "weekly/%s" % (lastmondayfile)
        if k.exists:
            s3bucket.delete_key(k)
        
    #backup daily
    print "Daily"
    conn = S3Connection(akey, skey)
    s3bucket = conn.get_bucket(bucket)
    k = Key(s3bucket)
    #upload data
    k.key = "daily/%s.tar.gz" % (filestamp)
    k.set_contents_from_filename("%s" % os.path.join(appfolder, filestamp+'.tar.gz'))
    print "Daily backup uploaded to s3"
    #delete file backup 4 hari kemarin
    last4days = datetime.date.today()-datetime.timedelta(4)
    last4daysfile = "%s-%s-%s.tar.gz" % (last4days.strftime('%Y'), last4days.strftime("%m"), last4days.strftime('%d'))
    k.key = "daily/%s" % (last4daysfile)
    if k.exists:
        s3bucket.delete_key(k)
    
    #delete folder temp
    deletefolder()
    
def deletefolder():
    if os.popen("rm -rf -R %s/%s*" % (appfolder,filestamp)):
        print "Folder Backup %s Deleted" % filestamp
    print "Backup completed"
        
if __name__ == "__main__":
	chkcfg()
