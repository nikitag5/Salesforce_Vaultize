#!c:/Python27/python.exe -u

print "Content-type: text/html\n\n";
import cgi
import requests
import json
from simple_salesforce import Salesforce
from data import SalesForceData
import sys
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import time
import Queue
import threading
import traceback
import os
import subprocess
import psutil

class SalesForceAuth(object):

    def __init__(self):

        self.lock = threading.Lock()

    def fillQueue(self):

        cmd=['mongorun','--dbpath','C:\mongodb\bin','--port',str(27017)]
        p = subprocess.Popen(cmd,shell = True)
        cmd1=['mongo','--dbpath','C:\mongodb\bin','--port',str(27017)]
        q = subprocess.Popen(cmd1,shell = True)

        client = MongoClient()
        db = client.mydb

        
        users = db.users

        self.q = Queue.Queue()

        # Insert each user's details in the queue
        
        for user in users.find():
            self.q.put(user)

        # Create as many threads as you want
        num_thread = 3
        self.threadList = []

        while not self.q.empty():

            with self.lock:
                dic = self.q.get()
                number = dic['_id']

            while 1:
                cnt = len(self.threadList)    
                if cnt < num_thread:
                    t = threading.Thread(target=self.getAuthDetails, args=(dic,number,), name=number)
                    t.start()
                    self.threadList.append(number)
                    print "THREAD ", number
                    break
                else:
                    time.sleep(15)
        
    # Get Authorization details of single user from the database 
    def getAuthDetails(self,dic,i):
        
        user_id = dic['_id']
        consumer_key = dic['clientId']
        consumer_secret = dic['clientSecret']
        refresh_token = dic['refreshToken']
        instance_url = dic['instanceURL']
        self.request_token_url = 'https://login.salesforce.com/services/oauth2/token'
        self.access_token_url = 'https://login.salesforce.com/services/oauth2/token'
        self.refresh_token_url = 'https://login.salesforce.com/services/oauth2/token'
            
            

        self.getQuery(dic,i)
    

    def getQuery(self,dic,i):

        try:
            
            consumer_key = dic['clientId']

            response = self.requestToken(dic)
            self.crawl(response,dic,i)
    
            self.threadList.remove(i)

            if not self.threadList:
                self.scheduling()

        except:

            fileDataException = open("E:\Backup\log.txt" ,"ab+")
            fileDataException.write("\nException\n")
            fileDataException.write(str(sys.exc_info()[1]))
            fileDataException.close()
            
        
    # Request for access token using refresh token   
    def requestToken(self,dic):
    
        data = {

                'grant_type': 'refresh_token',

                'client_id' : dic['clientId'],

                'client_secret' : dic['clientSecret'],

                'refresh_token' : dic['refreshToken']

            }

        headers = {

                        'content-type': 'application/x-www-form-urlencoded'

            }

        req = requests.post(self.refresh_token_url,data=data,headers=headers)

        response = req.json()

        return response


    def crawl(self,response,dic,i):

        # Create a salesforce instance
        sf = Salesforce(instance_url=dic['instanceURL'], session_id=response['access_token'])

        sessionID = response['access_token']

        sfd = SalesForceData(sf,dic['instanceURL'],sessionID)
        path1 = "E:\BEproject\Backup"
        path2 = "E:\BEproject\Backup\Note"
        path3 = "E:\BEproject\Backup\Attachment"
        sfd.createDirs([path1, path2, path3])
        sfd.processAllTables(i)

    def kill_proc_tree(pid, including_parent=True):
        parent = psutil.Process(pid)
        for child in parent.children(recursive=True):
            child.kill()
        if including_parent:
            parent.kill()

    #Checks whether the next crawl is to be implemented according to the frequency assigned   
    def scheduling(self):
        
        frequency = timedelta(minutes=2)
        last_crawl = datetime.now()
        
        flag = 4
        while (flag):
            
            current_time = datetime.now()
            
            if (current_time > (last_crawl + frequency)):
                self.fillQueue()
                last_crawl = datetime.now()

            else:
                time.sleep(30)



if __name__ == "__main__":

        sfo = SalesForceAuth()
        sfo.fillQueue()

         '''
        me = os.getpid()
        sfo.kill_proc_tree(me)
        '''

        
