import requests

validating_staging_schemas_api = "https://api-staging.llead.co/api/schemas/validate/"

staging_response = requests.post(validating_staging_schemas_api)

print("Staging schema validation finished!") if staging_response.ok else print("Staging schema validation failed!")
