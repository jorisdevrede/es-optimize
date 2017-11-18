import requests
import json

requests.delete('http://127.0.0.1:9200/test')

element = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Quae quo sunt excelsiores.'
body = element * 1000

params = {'settings': {'index': {'number_of_shards': 5, 'number_of_replicas': 0, 'refresh_interval': '-1'}}}

created = requests.put('http://127.0.0.1:9200/test', data=json.dumps(params))

print(str(created.status_code) + " " + created.text)

if created.status_code == 200:
    print('index test created')
    for i in range(100000):

        data = {'title': 'title',
                'body': body}

        response = requests.post('http://127.0.0.1:9200/test/mydocument', data=json.dumps(data))
        if response.status_code != 201:
            print(str(response.status_code) + " " + response.text)

        if i % 1000 == 0:
            print('documents added: {}'.format(i))

requests.put('http://127.0.0.1/test/_settings', data='{"refresh_interval": "5s"}')
