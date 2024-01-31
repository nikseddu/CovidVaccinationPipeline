from pathlib import Path
import pandas as pd
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from dotenv import load_dotenv
import datetime

from prefect_gcp import GcpCredentials


load_dotenv()


@task(name="Get files from the gcs")
def extract_from_gcs(state:str, year:int) -> Path:

    filename = f"{state}_{year}.parquet"
    gcs_block = GcsBucket.load("vaccination-block")
    gcs_block.get_directory(from_path=filename,local_path=f"data/{state}/")
    return Path(f"../data/{filename}")
    
@task(name="Transforming the data")
def transform(path: Path)->pd.DataFrame:
    """
    To do basic cleaing of the data before putting it into big query
    
    """
    print(path)
    df = pd.read_parquet(path)

    # Filtering for important columns
    cols =["date", "fips","recip_county","recip_state","administered_dose1_recip","administered_dose1_pop_pct","administered_dose1_recip_18plus","administered_dose1_recip_65plus", \
         "booster_doses", "series_complete_pop_pct_svi","series_complete_pop_pct", "metro_status","census2019","census2019_18pluspop","census2019_65pluspop"] 
    
    data = df.loc[:, df.columns.isin(cols)].sort_values(by= ["recip_state","recip_county","date"], ignore_index=True)


    
    #Renaming Columns To make more sense of the data

    changeName={'fips':'FIPS','date':'Date','recip_county':'County','recip_state':'State', 'administered_dose1_recip':'OneDoseComplete',\
                'administered_dose1_pop_pct':'OneDoseCompleteP','administered_dose1_recip_18plus':'OneDoseComplete18P',\
                'administered_dose1_recip_65plus':'OneDoseComplete65P'
                ,'booster_doses':'BoosterDone','series_complete_pop_pct':'CompleteDosagePercentage','series_complete_pop_pct_svi':'CompleteDosageWBoosterPercentage','census2019'\
                :'Population2019','census2019_18pluspop':'Population18P2019','census2019_65pluspop':'Population65P2019','metro_status':'MetroStatus'}
                
            
    data.rename(columns=changeName, inplace=True)

    #Enforcing the type
    type_matrix = {
    "Date" : "datetime64",
    "FIPS" : "category",
    "County": "string",
    "State" : "category",
    "MetroStatus" : "category"
        
    }

    def enforce_type(df, type_matrix):
        new_types = {col: type_matrix.get(col, "float") for col in df.columns}
        
        return df.astype(new_types)
    
    data = enforce_type(data,type_matrix)

    # Removing Unknown Counties 
    try:
        index_names = data[ data['County'] == "Unknown County"].index  
        data.drop(index_names, inplace = True)
    except:
        pass

    #imputation
    def fill_values(df):
    
        """
        This functions fills the missing values in dataframe 'df'
        
        """
        
        df["OneDoseComplete"] = pd.concat([ df["OneDoseComplete"].fillna(method='ffill'),  df["OneDoseComplete"].fillna(method='bfill')], axis=1).mean(axis=1)
        df["OneDoseComplete18P"] = pd.concat([ df["OneDoseComplete18P"].fillna(method='ffill'),  df["OneDoseComplete18P"].fillna(method='bfill')], axis=1).mean(axis=1)
        df["OneDoseComplete65P"] = pd.concat([ df["OneDoseComplete65P"].fillna(method='ffill'),  df["OneDoseComplete65P"].fillna(method='bfill')], axis=1).mean(axis=1)
        
            
        #For Filling the 65 Plus Population, I'm first counting the percentage poulation of 65 plus for the county with max Population(assuming 
        #Highest Population means highest number of 65 plus people)
        # Then imputing the same percentage out of total Population for that county( Which is in ~15% of the Total Population for that county)

        percentage = (df["Population65P2019"].max()/df["Population2019"].max())*100
        
        df["Population65P2019"] = df["Population2019"]*(percentage*10e-3)
        
        
        #For Booster Done df, It makes sense to fill 0 as 
        df["BoosterDone"].fillna(0.0, inplace=True) 
        df["CompleteDosageWBoosterPercentage"].fillna(0.0, inplace=True) 
    
    fill_values(data)

    return data



@task(name="Transfers the data to Big query")
def write_bq(df: pd.DataFrame) -> None:
    """
    Writes the data to big query
    """
    gcp_credentials_block = GcpCredentials.load("vaccination-gcp-creds")

    df.to_gbq(
        destination_table= "us.vaccination" ,
        project_id= "covidvaccinationpipeline",
        credentials=  gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow
def etl_gcs_to_bq(state: str,year:int)-> None:
    """
    Main etl flow for gcs to bq
    """

    path = extract_from_gcs(state,year)
    data = transform(path)
    write_bq(data)


@flow(name="parent flow for Data from Socrata to Google Cloud ")
def parent_flow() -> None:

    state = "NY" 
    year = 2021
    etl_gcs_to_bq(state,year)



if __name__ == "__main__":
    parent_flow()