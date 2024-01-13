from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

from extract import extract_data


data = extract_data()
print(data.head(2))


# @task(retries=3)
# def fetch(dataset_url: str) -> pd.DataFrame:
#     """Read taxi data from web into pandas DataFrame"""
#     # if randint(0, 1) > 0:
#     #     raise Exception

#     df = pd.read_csv(dataset_url)
#     return df