from poseidon import poseidon

accessKey = 'accessKey'
secretKey = 'secretKey'

url = 'https://{apigw-address}/{service-name}/{api-version}/{api_name}?{param1=value1&param2=value2}'

req = poseidon.urlopen(accessKey, secretKey, url)
print(req)