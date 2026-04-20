import requests
from requests.auth import HTTPBasicAuth

url = 'http://192.168.80.37:9201/'
index = 'gittba05_test'
type = '/_doc/'
id = 'LYHObJ0BZBoeEvh9AglU'

user = 'elastic'
password = 'pass4icai'

response = requests.get(
    url + index + type + id,
    auth=HTTPBasicAuth(user, password)
)

res = response.json()
print(res)