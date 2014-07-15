''' Script for extracting Salesforce user data '''

import simple_salesforce, sys, csv, base64, os, shutil, requests
from simple_salesforce import Salesforce
import pymongo
from pymongo import MongoClient

class SalesForceData(object):

        def __init__(self,sf,instanceURL,sessionId):
                
                self.sf=sf
                self.instance_url = instanceURL
                self.session_id = sessionId

        # Extract Notes and Attachments body from Note.csv and Attachment.csv resp.

        def saveFiles(self, tableNameObj, folder, record):

                if tableNameObj == 'Note':        
                        fileName = str(record['Title'])

                elif tableNameObj == 'Attachment':
                        fileName = str(record['Name'])

                fileData = open("E:\BEproject\Backup\%s\%s\%s" % (tableNameObj, folder, fileName), "wb+")

                if tableNameObj == 'Note':
                        # Type of body of a note is 'textarea'
                        fileData.write(str(record['Body']))
                elif tableNameObj == 'Attachment':
                        
                        temp = str(record['Body'])
                        url = self.instance_url+temp
                        
                        #Setup authorization passing the session id(i.e access token)
                        
                        headers1 = {
                                        'Authorization': 'OAuth ' + self.session_id
                                }
                        result = requests.get(url,headers=headers1)

                        #If the access token(session) has expired,use the refresh token to obtain a new access token
                        if str(result.status_code) == '401' :
                        
                                data = {

                                        'grant_type': 'refresh_token',

                                        'client_id' : self.consumer_key,

                                        'client_secret' : self.consumer_secret,

                                        'refresh_token' : self.refresh_token

                                }

                                headers = {

                                                'content-type': 'application/x-www-form-urlencoded'

                                }

                                req = requests.post(self.refresh_token_url,data=data,headers=headers)

                                response = req.json()
                                
                                accessToken = response['access_token']

                                headers2 = {
                                                'Authorization': 'OAuth ' + accessToken
                                        }
                                result = requests.get(url,headers=headers2)

                                
                        encoded = base64.b64encode(result.content)
                        
                        fileData.write(base64.b64decode(encoded))
                        # Type of body of an attachment is 'base64'.
                        # It has to be decoded for writing into the file.

                fileData.close()

        # Create directories where backed-up data will be stored.

        def createDirs(self, d):

                  '''
                if os.path.exists("E:\BEproject\Backup"):
                        shutil.rmtree("E:\BEproject\Backup")
                '''

        
                client = MongoClient()
                db = client.mydb
                attachments = db.attachments
                        
                if not os.path.exists("E:\BEproject"):
                        attachments.remove()
                        
                for each in d:
                        if not os.path.exists(each):
                                  os.makedirs(each)

        # Write table data into respective CSV file.

        def processQueryResult(self, queryResult, write, tableNameObj,w):

                try:

                        client = MongoClient()
                        db = client.mydb
                        attachments = db.attachments
                                
                        for record in queryResult["records"]:
                                recordFields1 = []
                                for key,value in record.iteritems():
                                        recordFields1.append(value)
                                del recordFields1[0]
                                write.writerow(recordFields1)


                        if tableNameObj == 'Attachment':

                                folder = tableNameObj+ str(w)

                                for record in queryResult["records"]:
                                        recordFields = {}
                                        for key,value in record.iteritems():
                                                recordFields[key] = value
                                        identifier = recordFields['Id']
                                        lastModified = recordFields['LastModifiedDate']
                                        data = recordFields
                                        data['user_id'] = w
                                        flag = 0

                                        #Check if new documents are inserted,if so then fetch them
                                        for attachment in attachments.find():
                                                if identifier == attachment['Id']:
                                                        flag = 1
                                                        break
                                        
                                        if flag == 0:
                                                attachments.insert(data)
                                                self.saveFiles(tableNameObj, folder, data)

                                        # Fetch the modified documents
                                        modified = attachments.find_and_modify(query = {'Id':identifier,'LastModifiedDate':{'$lt':lastModified}},update = data,new = True,upsert = False,multi = False)
                                        if modified != None:
                                                self.saveFiles(tableNameObj, folder, modified)
                                                
                except:
                        fileDataException = open("E:\BEproject\Backup\log.txt" ,"ab+")
                        fileDataException.write("\nException\n")
                        fileDataException.write(str(sys.exc_info()[1]))
                        fileDataException.close()
                

        # Retrieve data from all Salesforce Tables.

        def processAllTables(self,j):
		# describe will return list of all available Salesforce objects (tables).

                        k = j
                        w = j
                        tableList = ['Attachment','Note']

                        for tableNameObj in tableList:
                                res = self.sf.__getattr__(tableNameObj)

                                # describe() describes field list and object properties for the specified object.
                                o = res.describe()

                                fieldsToFetch = []

                                for i in o["fields"]:
                                        fieldsToFetch.append(str(i["name"]))

                                try:
                                        queryResult = self.sf.query("SELECT %s FROM %s" % (", ".join(fieldsToFetch), tableNameObj))
                                        # Write table data into CSV files using Python's CSV module
                                        tableName = tableNameObj+ str(k)
                                        tableFile = open("E:\BEproject\Backup\%s.csv" %tableName, "wb+")

                                        path4 = "E:\BEproject\Backup\%s\%s" % (tableNameObj, tableName)
                                        if not os.path.exists(path4):
                                                os.makedirs(path4)
                                                
                                        write = csv.writer(tableFile, delimiter = ',', quotechar = '"', quoting=csv.QUOTE_ALL)
                                        write.writerow(fieldsToFetch)

                                        self.processQueryResult(queryResult, write, tableNameObj,w)

                                        tableFile.close()

                                except:
                                        
                                        fileDataException = open("E:\BEproject\Backup\log.txt" ,"ab+")
                                        fileDataException.write("\nException\n")
                                        fileDataException.write(str(sys.exc_info()[1]))
                                        fileDataException.close()
                                        #print "\nException\n"
                                        #print sys.exc_info()[1]
