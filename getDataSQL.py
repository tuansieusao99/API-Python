import requests
import json
import csv
import cx_Oracle
from io import StringIO
from datetime import datetime

def getData():
    conn = cx_Oracle.connect('cargo_dev/cargo_dev2023@116.103.228.228:1521/dwhcargo')
    cursor = conn.cursor()
    # Enter View
    view = 'et4ae5__abTest__c'
    queryConfig = f"SELECT obj, v FROM Config WHERE v = '{view}'"
    cursor.execute(queryConfig)
    

    # Fetch the results
    row = cursor.fetchone()  # Fetches the first row that matches the query conditions

    # Check if a row was found
    if row:
        obj = row[0]  # Assuming obj is the first column (index 0)
        view = row[1]  # Assuming view is the second column (index 1)
        print(f"Obj: {obj}, View: {view}")
    else:
        print("No matching records found")
    # Execute query on the view
    query = f"SELECT * FROM {view}"
    cursor.execute(query)

    # Get column names from the view
    columns = [desc[0] for desc in cursor.description]

    # Fetch data
    data = cursor.fetchall()

    # Write data to CSV with ^ delimiter and \r\n for newlines
    current_time = datetime.now()
    formatted_time = current_time.strftime('%Y-%m-%d_%H-%M-%S')  # Format the timestamp
    file_name = f'{view}_{formatted_time}.csv'  # Create a modified file name
    with open(file_name, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter='^', lineterminator='\r\n')

        # Write header
        csvwriter.writerow(columns)

        # Write data
        csvwriter.writerows(data)

    # Read the content of the CSV file
    with open(file_name, 'r') as csvfile:
        dataSQL = csvfile.read()

    # Replace commas within double quotes with caret (^)
    inside_quotes = False
    dataSQL_list = list(dataSQL)

    # Reassemble the modified payload
    dataSQL = ''.join(dataSQL_list)

# Close connection
    cursor.close()
    conn.close()
    
    return [dataSQL,obj]
# getData()