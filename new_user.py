#!c:/Python27/python.exe -u

print "Content-type: text/html\n\n";
import cgi
import requests
import json
import pymongo
from pymongo import MongoClient

#For a new user whose credentials are not stored in the database
class Auth(object):

    def getDetails(self):

        client = MongoClient()
        db = client.mydb
        users = db.users

        user = users.find_one({'refreshToken': None})
        self.consumer_key = user['clientId']
        self.consumer_secret = user['clientSecret']
        self.instanceURL = user['instanceURL']
        self.user_id = user['_id']
        self.request_token_url = 'https://login.salesforce.com/services/oauth2/token'
        self.access_token_url = 'https://login.salesforce.com/services/oauth2/token'
        self.refresh_token_url = 'https://login.salesforce.com/services/oauth2/token'
        self.redirect_uri = 'http://localhost/cgi-bin/force_oauth.py'
        self.authorize_url = 'https://login.salesforce.com/services/oauth2/authorize?response_type=token&client_id='+self.consumer_key+'&redirect_uri='+self.redirect_uri

        self.getCode()
                
    def getCode(self):
        
        #Request for the authorization code
        query = cgi.FieldStorage()
        req = None
        re = "https://login.salesforce.com/services/oauth2/authorize?response_type=code&client_id="+self.consumer_key+"&redirect_uri="+self.redirect_uri

        if 'login' in query:
            
            print '<html>'
            print '  <head>'
            print '    <meta http-equiv="refresh" content="0;url=%s" />' % re
            print '    <title>You are going to be redirected</title>'
            print '  </head>' 
            print '  <body>'
            print '    Redirecting... <a href="%s"> </a>' % re
            print '  </body>'
            print '</html>'
            
        # Authorization code from the URL parameter is extracted and a POST request to authorization server is sent.
        # Refresh token is received from the JSON-encoded response payload 
        if 'code' in query:

            code = query.getvalue('code')
            
            data = {

                        'grant_type': 'authorization_code',

                        'redirect_uri': self.redirect_uri,

                        'code': code,

                        'client_id' : self.consumer_key,

                        'client_secret' : self.consumer_secret

                    }

            headers = {

                        'content-type': 'application/x-www-form-urlencoded'

                    }
            req = requests.post(self.access_token_url,data=data,headers=headers)

            response = req.json()

            refreshToken = response['refresh_token']
            print "refreshToken"
            print '<br>'
            print refreshToken

            #Insert the credentials in the database
            client = MongoClient()
            db = client.mydb
            users = db.users
            users.update({'_id': self.user_id},{'$set': {'refreshToken': refreshToken}})

            
if __name__ == "__main__":

        a = Auth()
        a.getDetails()

