import requests
import json
import csv
import cx_Oracle
from io import StringIO
# Enter API Object
obj = "et4ae5__abTest__c"
# Connect to Oracle database
conn = cx_Oracle.connect('cargo_dev/cargo_dev2023@116.103.228.228:1521/dwhcargo')
cursor = conn.cursor()

# Execute query on the view
query = f"SELECT * FROM {obj}"
cursor.execute(query)

# Get column names from the view
columns = [desc[0] for desc in cursor.description]

# Fetch data
data = cursor.fetchall()

# Write data to CSV with ^ delimiter and \r\n for newlines
with open('output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter='^', lineterminator='\r\n')

    # Write header
    csvwriter.writerow(columns)

    # Write data
    csvwriter.writerows(data)

# Read the content of the CSV file
with open('output.csv', 'r') as csvfile:
    payload1 = csvfile.read()

# Replace commas within double quotes with caret (^)
inside_quotes = False
payload1_list = list(payload1)
for i in range(len(payload1_list)):
    if payload1_list[i] == '"':
        inside_quotes = not inside_quotes
    elif inside_quotes and payload1_list[i] == ',':
        payload1_list[i] = '^'

# Reassemble the modified payload
payload1 = ''.join(payload1_list)

# Insert \r\n before each line break
#payload1 = payload1.replace('\n', '\\r\\n')

# Add double quotes at the beginning and end
#payload1 = '"' + payload1 + '"'

# Close connection
cursor.close()
conn.close()

# Now payload contains the content of the CSV file
print(payload1)  # Optional: Print the payload to verify
# GetToken
url = "https://test.salesforce.com/services/oauth2/token?username=admin.gms2@vna.sca.uat&password=Gms@2021&client_id=3MVG9vuHjyLKuxlHM_CSCtMjyTjo97_4jr.McXp_RU2RWmtVkLO4KbPuyRV2NJJLjRZmzpuYWWeZRMl5jBmcV&client_secret=B5E25E9F74DCFD1265D02A49846359FF59178ACF72737C0C0C92A56FA56973F5&grant_type=password"

payload = {}
headers = {
  'Cookie': 'BrowserId=5hmKbWdCEe64B7WrYr_tug; CookieConsentPolicy=0:0; LSKey-c$CookieConsentPolicy=0:0'
}

response = requests.request("POST", url, headers=headers, data=payload)
json_data = json.loads(response.text)

# Access the 'access_token' value
# json.load(response.text)
access_token = json_data['access_token']

# Print the access token
print(access_token)

url = "https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest"

payload = json.dumps({
  "operation": "insert",
  "object": obj,
  "contentType": "CSV",
  "lineEnding": "CRLF",
  "columnDelimiter": "CARET"
})
headers = {
  'Content-Type': 'application/json',
  'Authorization': f'Bearer {access_token}',
  'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
}

responseCreateJob = requests.request("POST", url, headers=headers, data=payload)

print(responseCreateJob.text)
json_data_job = json.loads(responseCreateJob.text)
print('job_id~~~~~~~~~~~~')
job_id = json_data_job['id']

# Print the access token
print(job_id)

url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}/batches"

payload ="DATE_TIME_API__C^DATE_API__C^TEXT_API__C^NUMBER_API__C^PICKLIST_API__C\r\n2023-11-06T12:34:56.000^^^^\r\n2023-11-06T12:34:56.000^2023-11-06^Tuan^123^A\r\n2023-11-06T12:34:56.000^2023-11-06^Tuan^123^A\r\n2023-11-06T12:34:56.000^2023-11-06^Tuan,Thang^123^A\r\n^2023-11-06^^^\r\n2023-11-06T12:34:56.000^^^^\r\n"
#payload = payload1
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print(payload)
#payload = "Text_API__c^Number_API__c^Account__c^Picklist_API__c^Date_API__c^Date_Time_API__c\r\nTuan^ho^1^0017F00004pW3SGQA0^A^2023-10-01^2023-11-06T12:34:56.000\r\nTuan1^2^0017F00004pW3SHQA0^B^2023-10-01^2023-11-06T12:34:56.000\r\nTuan2^3^0017F00004pW3SIQA0^C^2023-10-01^2023-10-01T09:58:12.000+0000\r\nTuan3^4^0017F00004pW3SJQA0^D^2023-10-01^2023-10-01T09:58:12.000+0000"
headers = {
  'Content-Type': 'text/csv',
  'Authorization': f'Bearer {access_token}',
}

responseUploadJob = requests.request("PUT", url, headers=headers, data=payload)

print(responseUploadJob.text)
url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}"

payload = json.dumps({
  "state": "UploadComplete"
})
headers = {
  'Content-Type': 'application/json',
  'Authorization': f'Bearer {access_token}',
  'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
}

responseClosedJob = requests.request("PATCH", url, headers=headers, data=payload)

print(responseClosedJob.text)

url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}"

payload = {}
headers = {
  'Authorization': f'Bearer {access_token}',
  'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
}

responseGetJobInfo = requests.request("GET", url, headers=headers, data=payload)

print(responseGetJobInfo.text)

url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}/successfulResults"

payload = {}
headers = {
  'Content-Type': 'application/json',
  'Authorization': f'Bearer {access_token}',
  'Cookie': 'BrowserId=_TF0G2v1Ee6z3Gsj4yQvPw; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1'
}



responseGetJobSS = requests.request("GET", url, headers=headers, data=payload)
csv_data = responseGetJobSS.text

# Create a CSV reader object
csv_reader = csv.reader(StringIO(csv_data))

# Convert CSV rows into a list of lists
data = [row for row in csv_reader]

# Print the data in the desired format
print(data)

# Specify the file name
file_name = "data_insert_success.csv"

# Writing to CSV file
with open(file_name, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print(f'Data exported to {file_name}')


url = f"https://vnacx--uat.sandbox.my.salesforce.com/services/data/v58.0/jobs/ingest/{job_id}/failedResults"

payload = {}
headers = {
    'Authorization': f'Bearer {access_token}',
}

responseFailed = requests.request("GET", url, headers=headers, data=payload)

print(responseFailed.text)
csv_dataFail = responseFailed.text

# Create a CSV reader object
csv_readerFail = csv.reader(StringIO(csv_dataFail))

# Convert CSV rows into a list of lists
dataFail= [row for row in csv_readerFail]

# Print the data in the desired format
print(dataFail)

# Specify the file name
file_Fail = "data_insert_fail.csv"
# Writing to CSV file
with open(file_Fail, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(dataFail)

print(f'Data exported to {file_Fail}')
print(payload1)