from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import requests



# Download and create dataset files
def download_dataset(name, url, output_folder="./data/", output_format=".csv"):
    # TODO: Need to handle situations where a dataset cannot be downloaded
    dataset = requests.get(url, allow_redirects=True)
    output_location = output_folder + name + output_format
    open(output_location, "wb").write(dataset.content)

    return output_location


def entry_point(**kwargs):
    print("Starting dataset downloads")

    datasets = kwargs["datasets"]

    for name in datasets:
        print("Downloading {} dataset".format(name))
        output = download_dataset(name, datasets[name])
        print("Downloaded {} dataset into {}".format(name, output))

    print("All datasets have been downloaded!")


def download_pp_complete(dag: DAG, datasets) -> PythonOperator:
    print_data_task = PythonOperator(
        task_id='download_pp_complete',
        python_callable=entry_point,
        op_kwargs={"datasets" : datasets},
        dag=dag,
    )

    return print_data_task
