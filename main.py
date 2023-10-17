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

SelectQuery = "select \"sophoraId\" from \"News\";"
cursor.execute(SelectQuery)
newsInDatabaseSopho = [tupel[0] for tupel in cursor.fetchall()]

SelectQuery = "select \"externalId\" from \"News\";"
cursor.execute(SelectQuery)
newsInDatabaseExter = [tupel[0] for tupel in cursor.fetchall()]


counter = 0
while api_url != None:
    print(f"Request {api_url}")

    # Senden Sie eine GET-Anfrage
    response = requests.get(api_url)

    # Überprüfen Sie, ob die Anfrage erfolgreich war (Status-Code 200)
    if response.status_code == 200:
        data = response.json()  # Die Antwort als JSON interpretieren

        for news in data['news']:
            if news.get('type') == 'video':
                continue
            if news['sophoraId'] in newsInDatabaseSopho and news['externalId'] in newsInDatabaseExter:
                continue
            allNews[news['sophoraId']] = news
            counter += 1
            if counter > 10:
                break

    else:
        print(f"Fehler: {response.status_code}")
        break

    try:
        api_url = data['nextPage']
    except KeyError:
        break

print("start inserting")

# insert new Tags
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

# insert new Types
SelectQuery = "select * from \"Types\";"
cursor.execute(SelectQuery)
typesInDataBase = [tupel[0] for tupel in cursor.fetchall()]

counter = 0
insertQuery = "INSERT INTO \"Types\" (\"name\") VALUES (%s) ON CONFLICT DO NOTHING;"
for news in allNews.values():
    if news['type'].lower() in typesInDataBase:
        continue
    cursor.execute(insertQuery, (news['type'].lower(),))
    counter += 1
write_log.write("New Types: " + str(counter) + "\n")

# Insert new News
insertQuery = """
    INSERT INTO "News"("sophoraId", "externalId", "title", date, "updateCheckUrl", "updateCheckUrlJSON", "breakingNews","topline", "details", "detailsJSON", "detailsweb", "detailswebPage", "shareURL", "shareURLPage", "fk_Type")
    VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
"""

counter = 0
errors = 0

for news in allNews.values():
    try:
        sophoraId = news['sophoraId']
        externalId = news['externalId']
        title = news['title']
        date = news['date']
        updateCheckUrl = news['updateCheckUrl']
        updateCheckUrlJSON = requests.get(news['updateCheckUrl']).text
        breakingNews = str(news['breakingNews'])
        topline = news['topline']
        details = news['details']
        detailsJSON = requests.get(news['details']).text
        detailsweb = news['detailsweb']
        detailswebPage = requests.get(news['detailsweb']).text
        shareURL = news['shareURL']
        shareURLPage = requests.get(news['shareURL']).text
        type = news['type'].lower()

        cursor.execute(insertQuery, (sophoraId, externalId, title, date, updateCheckUrl, updateCheckUrlJSON, breakingNews,topline, details, detailsJSON, detailsweb, detailswebPage, shareURL, shareURLPage, type))
    except Exception as e:
        print(e)
        errors += 1
        write_log.write("Error"+str(e) + "\n")

    conn.commit()
    counter += 1

write_log.write("New News: " + str(counter) + "\n")
write_log.write("Errors: " + str(errors) + "\n")

# Insert new TagsToNews
SelectQuery = "select * from \"Tags_News\";"
cursor.execute(SelectQuery)
tagsToNewsInDataBase = [tupel[0] for tupel in cursor.fetchall()]

counter = 0
insertQuery = "INSERT INTO \"Tags_News\" (\"Tags_name\", \"News_sophoraId\") VALUES (%s, %s) ON CONFLICT DO NOTHING;"
for news in allNews.values():
    for tag in news['tags']:
        if tag['tag'].lower() in tagsToNewsInDataBase:
            continue
        cursor.execute(insertQuery, (tag['tag'].lower(), news['sophoraId']))
        conn.commit()
        counter += 1



conn.close()

write_log.write("end:" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "\tNews:" + str(len(allNews)) + "\n")
