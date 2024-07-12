from datetime import datetime
import os
import requests
import psycopg2
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time

# log file run in log file which appends the current date and time
start_time = datetime.now()
if not os.path.exists("log"):
    os.makedirs("log")

write_log = None
if not os.path.exists("log/"+start_time.strftime("%m%Y")+"_log.txt"):
    write_log = open("log/"+start_time.strftime("%m%Y")+"_log.txt", "w")
else:
    write_log = open("log/"+start_time.strftime("%m%Y")+"_log.txt", "a")

write_log.write("\nstart:" + start_time.strftime("%d/%m/%Y %H:%M:%S") + "\n")

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
api_url = 'https://www.tagesschau.de/api2u/news/'

allNews = {}

SelectQuery = 'SELECT "sophoraId" FROM "News";'
cursor.execute(SelectQuery)
newsInDatabaseSopho = [tupel[0] for tupel in cursor.fetchall()]

counter = 0
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds


def fetch_data(url):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            retries += 1
            print(f"Connection error: {e}. Retrying {retries}/{MAX_RETRIES}...")
            time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Aborting.")
            break
    return None


while api_url is not None:
    print(f"Request {api_url}")

    data = fetch_data(api_url)

    if data:
        for news in data['news']:
            if news.get('type') == 'video':
                continue
            if news['sophoraId'] in newsInDatabaseSopho:
                continue
            if news.get('type') != 'video':
                allNews[news['sophoraId']] = news
    else:
        print("Failed to fetch data after multiple retries.")
        break

    api_url = data.get('nextPage') if data else None
    counter += 1
    if counter > 4:
        break

print("start inserting")

# tagsInDataBase
SelectQuery = 'SELECT * FROM "Tags";'
cursor.execute(SelectQuery)
tagsInDataBase = [tupel[0] for tupel in cursor.fetchall()]

# typesInDataBase
SelectQuery = 'SELECT * FROM "Types";'
cursor.execute(SelectQuery)
typesInDataBase = [tupel[0] for tupel in cursor.fetchall()]

# tagsToNewsInDataBase
SelectQuery = 'SELECT * FROM "Tags_News";'
cursor.execute(SelectQuery)
tagsToNewsInDataBase = [tupel[0] for tupel in cursor.fetchall()]

write_log.write("Adding " + str(len(allNews)) + " news to database\n")

lock = Lock()
submitted_counter = 0


def insert_news(news, index, total):
    global submitted_counter
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

    try:
        # insert new Tags
        insertQuery = 'INSERT INTO "Tags" ("name") VALUES (%s) ON CONFLICT DO NOTHING;'
        for tag in news['tags']:
            cursor.execute(insertQuery, (tag['tag'].lower(),))
            conn.commit()

        # insert new Types
        insertQuery = 'INSERT INTO "Types" ("name") VALUES (%s) ON CONFLICT DO NOTHING;'
        if type not in typesInDataBase:
            cursor.execute(insertQuery, (type,))
            conn.commit()

        # Insert new News
        insertQuery = '''
            INSERT INTO "News" ("sophoraId", "externalId", "title", date, "updateCheckUrl", "updateCheckUrlJSON", "breakingNews", "topline", "details", "detailsJSON", "detailsweb", "detailswebPage", "shareURL", "shareURLPage", "fk_Type")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        '''
        cursor.execute(insertQuery, (
        sophoraId, externalId, title, date, updateCheckUrl, updateCheckUrlJSON, breakingNews, topline, details,
        detailsJSON, detailsweb, detailswebPage, shareURL, shareURLPage, type))
        conn.commit()

        # Insert new TagsToNews
        insertQuery = 'INSERT INTO "Tags_News" ("Tags_name", "News_sophoraId") VALUES (%s, %s) ON CONFLICT DO NOTHING;'
        for tag in news['tags']:
            cursor.execute(insertQuery, (tag['tag'].lower(), sophoraId))
            conn.commit()

        with lock:
            submitted_counter += 1
            print(f"Inserting {index} of {total} types {sophoraId}")

    except Exception as e:
        print("ERROR" + str(e))
        write_log.write("ERROR" + str(e) + "\n")


total_news = len(allNews)
with ThreadPoolExecutor(max_workers=10) as executor:
    for index, currentNews in enumerate(allNews.values(), start=1):
        executor.submit(insert_news, currentNews, index, total_news)

write_log.write("finished:" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "\tRuntime: " + str(
    datetime.now() - start_time) + "\n")
write_log.close()
conn.close()
