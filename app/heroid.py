import requests
import json


hero_data = requests.get('https://api.opendota.com/api/heroes')
data = json.loads(hero_data.text)
print(json.dumps(data, indent=4))