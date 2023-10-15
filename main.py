from datetime import datetime

import os

try:
    import requests
except ImportError:
    os.system('python -m pip install requests')
import requests

try:
    import psycopg2
except ImportError:
    os.system('python -m pip install psycopg2')
import psycopg2

try:
    import json
except ImportError:
    os.system('python -m pip install json')
import json



# log filerun in log file which appends the current date and time

write_log = open("log.txt", "a")
write_log.write("\nstart:" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "\n")

# read json file
with open('settings.json') as json_file:
    data = json.load(json_file)

# Postgresql connection
print("Connecting to database")
conn = psycopg2.connect(
    host=data['database']['host'],
    port=data['database']['port'],
    database=data['database']['database'],
    user=data['database']['user'],
    password=data['database']['password']
)
cursor = conn.cursor()
print("Connected to database")

# URL der API
api_url = 'https://www.tagesschau.de/api2/news/'

allNews = {}

while api_url != None:
    print(f"Request {api_url}")

    # Senden Sie eine GET-Anfrage
    response = requests.get(api_url)

    # Überprüfen Sie, ob die Anfrage erfolgreich war (Status-Code 200)
    if response.status_code == 200:
        data = response.json()  # Die Antwort als JSON interpretieren

        for news in data['news']:
            allNews[news['sophoraId']] = news
    else:
        print(f"Fehler: {response.status_code}")
        break

    try:
        api_url = data['nextPage']
    except KeyError:
        break

#insert new Tags
SelectQuery = "select * from \"Tags\";"
cursor.execute(SelectQuery)
tagsInDataBase = [tupel[0] for tupel in cursor.fetchall()]

counter = 0
insertQuery = "INSERT INTO \"Tags\" (\"name\") VALUES (%s) ON CONFLICT DO NOTHING;"
for news in allNews.values():
    for tag in news['tags']:
        if tag['tag'].lower() in tagsInDataBase:
            continue
        cursor.execute(insertQuery, (tag['tag'].lower(),))
        conn.commit()
        counter += 1

write_log.write("New Tags: " + str(counter) + "\n")

conn.close()

write_log.write("end:" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "\tNews:" + str(len(allNews)) + "\n")
print(len(allNews))
