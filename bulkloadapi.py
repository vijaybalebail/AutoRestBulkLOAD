import os
import sys
import time
import threading
import requests
import requests.auth
import glob

CLIENT_ID = "vUAMxAaryiATKFviEFmiaA.."
CLIENT_SECRET = "tQvr68ohioK488kEOQP1eQ.."
TOKEN_URL = "https://fp7cb75hkszpygo-db202201121316.adb.us-sanjose-1.oraclecloudapps.com/ords/admin/oauth/token"
REDIRECT_URI = "https://www.getpostman.com/oauth2/callback"
FILE_URL = 'https://fp7cb75hkszpygo-db202201121316.adb.us-sanjose-1.oraclecloudapps.com/ords/admin/cust/batchload?batchRows=5000&errorsMax=20&timestampFormat=YYYY-MM-DD HH24:MI:SS&dateFormat=YYYY-MM-DD HH24:MI:SS'
directory = "C:/test/"
FILE_NAME = 'cust*.csv'  # default
chunk_size = 4

if len(sys.argv) > 1:
    argument = sys.argv[1]
    print("The argument you passed is:", argument)
    FILE_NAME = argument
else:
    print("No argument was passed.")
    print("Enter: python pt2.py <file_name>.csv")
    print("E.g.: python pt2.py customer*.csv")
    time.sleep(3)

def get_access_token(url, client_id, client_secret):
    response = requests.post(url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
    )
    return response.json()["access_token"]

headers = {
    "Authorization": "Bearer " + get_access_token(TOKEN_URL, CLIENT_ID, CLIENT_SECRET),
    "Content-Type": "application/csv"
}

def make_post_request(url, data):
    response = requests.post(url, data=open(data, 'rb'), headers=headers)
    if response.status_code == 200:
        print("Successful POST request")
        print(response)
        print(response.text)
    else:
        print("Failed POST request")
        print(response)

def run_threads(url, data_list):
    threads = []
    for data in data_list:
        thread = threading.Thread(target=make_post_request, args=(url, data))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    print("All files in threads have finished")

data_list = glob.glob(os.path.join(directory, FILE_NAME))

for data in data_list:
    print(os.path.join(directory, data))


start = time.time()
#run_threads(FILE_URL, data_list)
#for data in data_list:

for i in range(0, len(data_list), chunk_size):
    small_array = data_list[i:i+chunk_size]
    run_threads(FILE_URL, small_array)
    print(small_array)


FILE_NAME = "/home/opc/a.csv"
headers = {'Content-Type': 'text/csv'}
end = time.time()
print("Created")
print(end - start)
time.sleep(1)