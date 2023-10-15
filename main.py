from datetime import datetime

import requests

#log filerun in log file which appends the current date and time

write_log = open("log.txt", "a")
write_log.write("start:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S")+"\t")

# URL der API
api_url = 'https://www.tagesschau.de/api2/news/'

allNews = {}

while api_url != None:
    print(f"Anfrage an {api_url}")

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

write_log.write("end:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S")+"\tNews:"+str(len(allNews))+"\n")
print(len(allNews))
