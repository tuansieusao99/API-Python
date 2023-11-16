import requests
import json
import csv
import cx_Oracle
import getDataSQL
from io import StringIO
from datetime import datetime
# Now payload contains the content of the CSV file
# print(payload1)  # Optional: Print the payload to verify
# GetToken
def pushBulkAPI():
    dataSQL = getDataSQL.getData()[0]
    obj = getDataSQL.getData()[1]
    url = "https://test.salesforce.com/services/oauth2/token?username=admin.gms2@vna.sca.uat&password=Gms@2021&client_id=3MVG9vuHjyLKuxlHM_CSCtMjyTjo97_4jr.McXp_RU2RWmtVkLO4KbPuyRV2NJJLjRZmzpuYWWeZRMl5jBmcV&client_secret=B5E25E9F74DCFD1265D02A49846359FF59178ACF72737C0C0C92A56FA56973F5&grant_type=password"

    payloadGetToken = {}
    headers = {
    'Cookie': 'BrowserId=5hmKbWdCEe64B7WrYr_tug; CookieConsentPolicy=0:0; LSKey-c$CookieConsentPolicy=0:0'
    }

    response = requests.request("POST", url, headers=headers, data=payloadGetToken)
    json_data = json.loads(response.text)

    # Access the 'access_token' value
    # json.load(response.text)
    access_token = json_data['access_token']

    # Print the access token
    print(access_token)

    url = "https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest"

    payloadCreateJob = json.dumps({
    "operation": "insert",
    "object": obj,
    "contentType": "CSV",
    "lineEnding": "LF",
    "columnDelimiter": "CARET"
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {access_token}',
    'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
    }

    responseCreateJob = requests.request("POST", url, headers=headers, data=payloadCreateJob)

    print(responseCreateJob.text)
    json_data_job = json.loads(responseCreateJob.text)
    print('job_id')
    job_id = json_data_job['id']

    # Print the access token
    print(job_id)

    url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}/batches"

    headers = {
    'Content-Type': 'text/csv',
    'Authorization': f'Bearer {access_token}',
    }

    responseUploadJob = requests.request("PUT", url, headers=headers, data=dataSQL)
    # print("responseUploadJob.text")
    # print(responseUploadJob.text)
    url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}"

    payloadClose = json.dumps({
    "state": "UploadComplete"
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {access_token}',
    'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
    }

    responseClosedJob = requests.request("PATCH", url, headers=headers, data=payloadClose)
    print("responseClosedJob.text")
    print(responseClosedJob.text)
    return(access_token,job_id)


#     url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}"

#     payloadGetInfo = {}
#     headers = {
#     'Authorization': f'Bearer {access_token}',
#     'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
#     }

#     responseGetJobInfo = requests.request("GET", url, headers=headers, data=payloadGetInfo)

#     # print(responseGetJobInfo.text)

#     url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}/successfulResults"

#     payloadSS = {}
#     headers = {
#     'Content-Type': 'application/json',
#     'Authorization': f'Bearer {access_token}',
#     'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
#     }



#     responseGetJobSS = requests.request("GET", url, headers=headers, data=payloadSS)
#     csv_data = responseGetJobSS.text

#     # Create a CSV reader object
#     csv_reader = csv.reader(StringIO(csv_data))

#     # Convert CSV rows into a list of lists
#     data = [row for row in csv_reader]
#     # Specify the file name
#     file_name = "data_insert_success.csv"

#     # Writing to CSV file
#     with open(file_name, mode='w', newline='') as file:
#         writer = csv.writer(file)
#         writer.writerows(data)
        
#     url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}/failedResults"

#     payloadFail = {}
#     headers = {
#         'Authorization': f'Bearer {access_token}',
#     }

#     responseFailed = requests.request("GET", url, headers=headers, data=payloadFail)
#     csv_dataFail = responseFailed.text

#     # Create a CSV reader object
#     csv_readerFail = csv.reader(StringIO(csv_dataFail))

#     # Convert CSV rows into a list of lists
#     dataFail= [row for row in csv_readerFail]

#     # Specify the file name
#     file_Fail = "data_insert_fail.csv"
#     # Writing to CSV file
#     with open(file_Fail, mode='w', newline='') as file:
#         writer = csv.writer(file)
#         writer.writerows(dataFail)
        
#     return(access_token,job_id)
# pushBulkAPI()