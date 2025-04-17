import jwt
from cryptography.hazmat.primitives import serialization
import time
import secrets

key_name       = "organizations/32eb8db2-c903-4e05-ad8f-364aaba57abc/apiKeys/a3d644d7-e3b9-415d-8fd0-6e21b134075a"
key_secret     = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIDVtwQe7UrJs6CLIPUnTnO7yaGZe0ApStBdNt1CfgtZkoAoGCCqGSM49\nAwEHoUQDQgAE8NCwJj9td0GBnvZfjGasrjxjC2pHlSM4hafan+ThKJqsYCga9tvS\nsHn5X77vJTNi9xKr2kD38Nhu2Y9TSRXpCQ==\n-----END EC PRIVATE KEY-----\n"
request_method = "GET"
request_host   = "api.coinbase.com"
request_path   = "/api/v3/brokerage/products/BTC-USD"
def build_jwt(uri):
    private_key_bytes = key_secret.encode('utf-8')
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None)
    jwt_payload = {
        'sub': key_name,
        'iss': "cdp",
        'nbf': int(time.time()),
        'exp': int(time.time()) + 120,
        'uri': uri,
    }
    jwt_token = jwt.encode(
        jwt_payload,
        private_key,
        algorithm='ES256',
        headers={'kid': key_name, 'nonce': secrets.token_hex()},
    )
    return jwt_token
def main():
    uri = f"{request_method} {request_host}{request_path}"
    jwt_token = build_jwt(uri)
    print(jwt_token)
if __name__ == "__main__":
    main()