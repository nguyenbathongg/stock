import os
import json
import requests
import datetime

FUNCTION = "MARKET_STATUS"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

url = f'https://www.alphavantage.co/query?function={FUNCTION}&apikey={API_KEY}'
response = requests.get(url)
data = response.json()['markets']

json_object = json.dumps(data, indent=4)

# Create the directory if it doesn't exist
os.makedirs("./backend/data/raw/markets", exist_ok=True)

# Get today's date and format it for the filename
date = datetime.date.today().strftime("%Y-%m-%d")
path = f"./backend/data/raw/markets/crawl_markets_{date}.json"

with open(path, "w") as outfile:
    outfile.write(json_object)
print(f"Data saved to {path}")

