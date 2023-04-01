import requests

datasets = {
    "housing": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv",
}

# Download and create dataset files
def download_dataset(name, url, output_folder="./data/", output_format=".csv"):
    # TODO: Need to handle situations where a dataset cannot be downloaded
    dataset = requests.get(url, allow_redirects=True)
    output_location = output_folder + name + output_format
    open(output_location, "wb").write(dataset.content)

    return output_location


def main():
    print("Starting dataset downloads")

    for name in datasets:
        print("Downloading {} dataset".format(name))
        output = download_dataset(name, datasets[name])
        print("Downloaded {} dataset into {}".format(name, output))

    print("All datasets have been downloaded!")


if __name__ == "__main__":
    main()