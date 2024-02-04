from pathlib import Path
import pandas as pd
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from sodapy import Socrata 
from dotenv import load_dotenv
from requests.exceptions import RequestException
from datetime import timedelta,date
import datetime
from prefect.tasks import task_input_hash

load_dotenv()

@task(name="Get the client for the socrata")
def get_client(path :str) -> any:
    socrataToken = os.getenv("socrataToken")
    socrataUserName = os.getenv("socratausername")
    socrataPassword = os.getenv("socratapassword")


    client = Socrata(path,
                 socrataToken,
                 username=socrataUserName,
                 password=socrataPassword)
    
    return client


@task(retries=3, name="Will fetch the data using socrata API",log_prints=True,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def fetch(client : any, states: str, year:int) -> pd.DataFrame:
       
    try :
        start_date = datetime.date(year, 1,1)
        end_date = datetime.date(year,12,31)
        condition = f"date between '{start_date}' and '{end_date}'"

        results = client.get("8xkx-amqh", recip_state=states, where= condition,limit=100000)
        
        df = pd.DataFrame.from_records(results)

        print(df.head(2))

        return df
    
    except RequestException as e:
        print("Error getting data from the client")
        raise 


@task(name="Basic preprocessing")
def clean(data : pd.DataFrame) -> pd.DataFrame:
    data["date"]=pd.to_datetime(data['date'])
    return data


@task(name="Writing files locally")
def write_to_local(df:pd.DataFrame,state: str, path:Path) -> Path:

    """
    Write to local as a parquet file
    
    """
    default_directory = os.getenv("PROJECT_DIRECTORY")
    path = Path(f"{default_directory}/data/{state}/{path}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True) # MAking the country folder if doesn't exits
    df.to_parquet(path,compression="gzip") 
    return path


@task(retries=1, name="Will push the data to the google cloud storage")
def write_to_gcs(path : Path) -> None:
    """
    Write the parquet file to the Google cloud
    
    """
    gcs_block = GcsBucket.load("vaccination-block")
    gcs_block.upload_from_path(from_path = f"{path}")
    
    return
    

@flow(name="Data from Socrata to Google Cloud")
def etl_web_to_gcs(state: str,year: int) -> None:
    """
    Main flow 
    
    """
    apiPath = "data.cdc.gov"

    client = get_client(apiPath) # Getting the Socrata client

    data = fetch(client, state,year)  # Fetching the data from api


    data = clean(data) # basic preprocessing

    dataset_file = f"{state}_{year}"

    path = write_to_local(data,state, dataset_file) 
    
    write_to_gcs(path) # Writing to gcs

    pass

@flow(name="parent flow for Data from Socrata to Google Cloud ")
def parent_flow_web_to_gcs( states : list[str] = ["NY", "NC"] , year : int = 2021) -> None:

    for state in states:
        etl_web_to_gcs(state,year)

if __name__ == "__main__":
    
    states = ["NY", "NC"]
    year = 2021
    parent_flow_web_to_gcs(states, year)