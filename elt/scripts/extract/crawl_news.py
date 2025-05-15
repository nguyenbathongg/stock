import requests
import json
import datetime
import os

FUNCTION = "NEWS_SENTIMENT"

def get_data_by_time_range(time_zone):
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    if time_zone == 1:
        time_from = yesterday.strftime("%Y%m%dT" + "0000")
        time_to = yesterday.strftime("%Y%m%dT" + "0929")
    elif time_zone == 2:
        time_from = yesterday.strftime("%Y%m%dT" + "0930")
        time_to = yesterday.strftime("%Y%m%dT" + "1600")
    else:
        time_from = yesterday.strftime("%Y%m%dT" + "1601")
        time_to = yesterday.strftime("%Y%m%dT" + "2359")

    return time_from, time_to

sort = "LATEST"
limit = "1000"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

json_object = []
total = 0

for time_zone in [1, 2, 3]:
    time_from, time_to = get_data_by_time_range(time_zone)
    print(f"Fetching data from {time_from} to {time_to} for time zone {time_zone}")

    url = f"https://www.alphavantage.co/query?function={FUNCTION}&time_from={time_from}&time_to={time_to}&limit={limit}&apikey={API_KEY}"

    response = requests.get(url)

    data = response.json()["feed"]

    json_object += data
    total += len(data)

json_object = json.dumps(json_object, indent=4)

date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")

path = f"/home/thong/WorkSpace/Project/elt/data/raw/news/crawl_news_{date}.json"

with open(path, "w") as outfile:
    outfile.write(json_object)
print(f"Total records fetched: {total}")
print(f"Data saved to {path}")