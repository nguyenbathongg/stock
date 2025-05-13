import requests
import json
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

exchanges = ["nyse", "nasdaq"]

list_companies = []

# Loop through each exchange and fetch the company data
for exchange in exchanges:
    url = f"https://api.sec-api.io/mapping/exchange/{exchange}?token={os.getenv('SEC_API_KEY')}"
    response = requests.get(url)
    data = response.json()
    list_companies.extend(data)
    print(f"Extracted {len(data)} companies from {exchange.upper()} exchange.")


# Create the directory if it doesn't exist
os.makedirs("./backend/data/raw/companies", exist_ok=True)

# Get today's date and format it for the filename
date = datetime.date.today().strftime("%Y-%m-%d")
path = f"./backend/data/raw/companies/crawl_companies_{date}.json"

#Serialize the list of companies to JSON and save it to a file
json_object = json.dumps(list_companies, indent=4)

with open(path, "w") as outfile:
    outfile.write(json_object)
print(f"Data saved to {path}")
