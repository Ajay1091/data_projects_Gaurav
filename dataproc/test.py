import requests
import configuration as conf



response = requests.get(conf.URL_MONTH)

if response.status_code == 200:
    content = response.json()

    print(content)
else:
    print('check path correctly')
print('-------------------------------------------------')
print(type(content))