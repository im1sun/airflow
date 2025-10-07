import requests
# client_id, authorize_code 노출 주의, 실제 값은 임시로만 넣고 Git에 올라가지 않도록 유의

client_id = '9dd1ba37254bce5b9c64105da44d12a5'
redirect_uri = 'https://example.com/oauth'
authorize_code = 'uoCUp8-FE4LrXDGaNpOegkoPX6VcdXyqrGEgeb8RvqGg1XBNzrg2YgAAAAQKDRWbAAABmb63oqN-jFVpBnvzXw'


token_url = 'https://kauth.kakao.com/oauth/token'
data = {
    'grant_type': 'authorization_code',
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'code': authorize_code,
    }

response = requests.post(token_url, data=data)
tokens = response.json()
print(tokens)
