import requests
import time

url = "https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR"

def get_stream(url):
    data = requests.Session()

    with data.get(url, headers=None, stream=True) as response:
        for line in response.iter_lines():
            if line:
                print(line)

while True:
    get_stream(url),
    time.sleep(1)