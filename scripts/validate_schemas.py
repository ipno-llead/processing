import requests

validating_staging_schemas_api = "https://api-staging.llead.co/api/schemas/validate/"

response = requests.post(validating_staging_schemas_api)

print("Validation finished!") if response.ok else print("Validation failed!")
