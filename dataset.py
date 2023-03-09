import requests

url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
name = "./data/housing_dataset.csv"
dataset = requests.get(url, allow_redirects=True)


open(name, "wb").write(dataset.content)