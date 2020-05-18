##################### SCRIPT FOR UPDATING TABLES IN QB APPS WITH DATA FROM SQL SERVER #####################
################################## written by Aleksandra Cybulska ##################################

## INSTRUCTIONS
# Modify the variables in the next section as specified below. All variables need to be inside single quotation marks, except sql_query:
#table_id         ID of the table in QB app where you want to insert the data. Example: 'bp3dm9xwf'
#token            User token in QB assigned to the app where you want to insert the data. Example: 'xxxxb4u895_uyp_b7ssdpxxx'
#cols             List of QB column IDs where the data needs to be send.
#                   Numbers need to be separated by commas and the column order should correspond to the column order in the SQL query.
#                   Example: [6, 7, 8, 9, 10] will send the first column from the SQL query to the column with ID 6 in QuickBase.
#sql_query        SQL query text. The query needs to be preceded and followed by three single quotation marks. Example: '''SELECT * FROM MyTable'''
#realm_hostname   QB realm hostname, for example 'mycompany.quickbase.com

#log_table_id     ID of the table in QB app where you want to insert your log data (optional)
#log_cols         List of QB column IDs where the log data needs to be send.(optional)

#LASTLY, please do a Ctrl+F to search for the "Replace below!" string so that you can see the Driver & Server values that you should replace with your own. Additionally, we provide you with "Option 2" to write your log data to a SQL table (instead of a QB table) and you will need a Database & Schema name if you want to go this route

## VARIABLES
table_id = ''
token = ''
cols = [6, 7, 8, 9, 10]
sql_query = ''' '''
realm_hostname = ''

log_table_id = ''
log_cols = [6, 7, 8, 9, 10]


### IMPORTING PACKAGES
import pandas as pd
import sqlalchemy as sqla
import numpy as np
import requests
import unicodedata
from datetime import datetime
import urllib
import json



### EXTRACT
conn_str = (
    #Replace below!... input your SQL Server driver
    r'Driver=ODBC Driver 13 for SQL Server;'
    #Replace below!... input your server name
    r'Server=YOUR_SERVER_HERE;'
    r'Trusted_Connection=yes;'
    #r'UID=username;'
    #r'PWD=password'
    )

quoted_conn_str = urllib.parse.quote_plus(conn_str)
engine = sqla.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_conn_str))

with engine.connect() as sql_conn:
    mydata = pd.read_sql(sql_query, sql_conn)

del conn_str, sql_conn
print('Data import completed')


### TRANSFORM

# Replacing diacritics with ASCII characters
def decoder(x):
    x = unicodedata.normalize('NFKD', x).encode('ascii', 'ignore')
    x = x.decode('utf-8')
    return x


for column in mydata.columns:
    mydata[column] = mydata[column].astype(str).apply(decoder)


str_cols = [str(x) for x in cols]
mydata.columns = str_cols


### LOAD
# Deleting all records from the existing QB table
print('Sending  Delete Records API request...')
headers = {'QB-Realm-Hostname': realm_hostname, 'Authorization': 'QB-USER-TOKEN ' + token}
data = eval(json.dumps({"from": table_id, "where": r'{3.GT.0}'}))
r1 = requests.delete(url='https://api.quickbase.com/v1/records', headers=headers, json=data)


# Preparing for export
step = 50000
dflength = len(mydata.index)
iter_np = np.arange(0, dflength, step)
iter = list(iter_np)

def slice_df(mydata, start_line):
    end_line = start_line + step
    slice = mydata.iloc[start_line:end_line,:]
    return slice

req_total = int(np.ceil(dflength / step))
req_nr = 1

if str(r1) == '<Response [200]>':
    errorlog1 = '0 no error '
else:
    errorlog1 = 'FAILED to delete records '
errorlog2 = ""

# Loading Data to a QB table
for i in iter :
    slice = slice_df(mydata, i)
    print('Sending Insert/ Update Records API request ' + str(req_nr) + ' out of ' + str(req_total))
    df_json = slice.to_json(orient='records')
    df_json = json.loads(df_json)
    df_json = [{key: {"value": value} for key, value in item.items()} for item in df_json]
    headers = {'QB-Realm-Hostname': realm_hostname, 'Authorization': 'QB-USER-TOKEN ' + token}
    data = {"to": table_id, "data": df_json}
    r = requests.post(url='https://api.quickbase.com/v1/records', headers = headers, json = data)
    print(str(r))
    if str(r) == '<Response [200]>':
        err_code = '0 no error '
    else:
        err_code = 'ERROR import failed '
    errorlog2 += err_code
    req_nr += 1


print('Delete records request: ' + errorlog1 + "\nExport to Quickbase requests: " + errorlog2)



# Creating log data

log_data = {'Upload Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Process Name': "QB Upload",
            'TableID': table_id,
            'Delete-records Error Code': errorlog1,
            'Insert/Update-records Error Code': errorlog2}

log_data = pd.DataFrame(log_data,  index=[0])

# OPTION 1.... Loading log data to a QB table

str_cols = [str(x) for x in log_cols]
log_data.columns = str_cols

log_json = log_data.to_json(orient='records')
log_json = json.loads(log_json)
log_json = [{key: {"value": value} for key, value in item.items()} for item in log_json]
headers = {'QB-Realm-Hostname': realm_hostname, 'Authorization': 'QB-USER-TOKEN ' + token}
data = {"to": log_table_id, "data": log_json}
r3 = requests.post(url='https://api.quickbase.com/v1/records', headers=headers, json=data)

if str(r3) == '<Response [200]>':
    print('Log data table has been updated')
else:
    print('FAILED to upload log data to QB')


# OPTION 2.... Loading log data to a SQL table... uncomment the below lines if you want to leverage!

#conn_str = (
#    #Replace below!... input your SQL Server driver
#    r'Driver=ODBC Driver 13 for SQL Server;'
#    #Replace below!... input your server name
#    r'Server=YOUR_SERVER_HERE;'
#    #Replace below!... input your server name
#    r'Database=YOUR_DATABASE_HERE;'
#    r'Trusted_Connection=yes:'
#    #r'UID=username;'
#    #r'PWD=password'
#    )
#
#quoted_conn_str = urllib.parse.quote_plus(conn_str)
#engine = sqla.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_conn_str))
#
#with engine.connect() as sql_conn:
#    log_data.to_sql(name='python_upload_log',
#                    con=sql_conn,
#                    #Replace below!... input your schema name
#                    schema='YOUR_SCHEMA_NAME',
#                    if_exists='append',
#                    index=False,
#                    dtype={'Upload Date': sqla.types.DateTime(),
#                           'Process Name': sqla.types.String(),
#                           'TableID': sqla.types.String(),
#                           'Delete-records Error Code': sqla.types.String(),
#                           'Insert/Update-records Error Code': sqla.types.String()}
#                    )


print('Process completed')
