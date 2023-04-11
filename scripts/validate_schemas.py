import requests

validating_schemas_api = "https://api.llead.co/api/schemas/validate/"

response = requests.post(validating_schemas_api)

print("Schema validation finished!") if response.ok else print("Schema validation failed!")
