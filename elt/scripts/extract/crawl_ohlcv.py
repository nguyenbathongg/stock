import os
import requests
import json
import datetime

date_crawl = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

API_KEY = os.getenv("POLYGON_API_KEY")

# Set the parameters for the API request
adjusted = "true"
include_otc = "true"

# Contruct the URL for the API request
url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_crawl}?adjusted={adjusted}&include_otc={include_otc}&apiKey={API_KEY}"

response = requests.get(url)

data = response.json()
data = data.get("results", [])

json_object = json.dumps(data, indent=4)

date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")

path = f"/home/thong/WorkSpace/Project/elt/data/raw/ohlcv/crawl_ohlcv_{date}.json"

with open(path, "w") as outfile:
    outfile.write(json_object)
print(f"Total records fetched: {len(data)}")
print(f"Data saved to {path}")

