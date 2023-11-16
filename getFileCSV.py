import requests
import json
import csv
import cx_Oracle
import getDataSQL
import push_Bulk_API
from io import StringIO
from datetime import datetime
def getFileSS():
    job_id = push_Bulk_API.pushBulkAPI()[1]
    access_token = push_Bulk_API.pushBulkAPI()[0]
    url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}"

    payloadGetInfo = {}
    headers = {
    'Authorization': f'Bearer {access_token}',
    'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
    }

    responseGetJobInfo = requests.request("GET", url, headers=headers, data=payloadGetInfo)

    # print(responseGetJobInfo.text)

    url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}/successfulResults"

    payloadSS = {}
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {access_token}',
    'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
    }



    responseGetJobSS = requests.request("GET", url, headers=headers, data=payloadSS)
    csv_data = responseGetJobSS.text

    # Create a CSV reader object
    csv_reader = csv.reader(StringIO(csv_data))

    # Convert CSV rows into a list of lists
    data = [row for row in csv_reader]
    # Specify the file name
    file_name = "data_insert_success.csv"

    # Writing to CSV file
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)
        return(file_name)
    
def getFileFail(): 
    job_id = push_Bulk_API.pushBulkAPI()[1]
    access_token = push_Bulk_API.pushBulkAPI()[0]   
    url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}/failedResults"

    payloadFail = {}
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    responseFailed = requests.request("GET", url, headers=headers, data=payloadFail)
    csv_dataFail = responseFailed.text

    # Create a CSV reader object
    csv_readerFail = csv.reader(StringIO(csv_dataFail))

    # Convert CSV rows into a list of lists
    dataFail= [row for row in csv_readerFail]

    # Specify the file name
    file_Fail = "data_insert_fail.csv"
    # Writing to CSV file
    with open(file_Fail, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(dataFail)
        return(file_Fail)
    
getFileSS()
getFileFail()
