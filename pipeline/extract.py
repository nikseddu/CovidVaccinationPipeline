import pandas as pd
import numpy as np 

from sodapy import Socrata # Using Socrata API
from dotenv import load_dotenv

import os


load_dotenv()

socrataToken = os.getenv("socrataToken")
socrataUserName = os.getenv("socratausername")
socrataPassword = os.getenv("socratapassword")



client = Socrata("data.cdc.gov",
                 socrataToken,
                 username=socrataUserName,
                 password=socrataPassword)


aStates = ["NY", "AL"] 

def extract_data() -> pd.DataFrame :
    for sStates in aStates:
        
        
        #Getting the data only for 2021
        
        results = client.get("8xkx-amqh", recip_state=sStates, where="date between '2021-01-01' and '2021-12-31'",limit=40000)

        df= pd.DataFrame()

        df_state = pd.DataFrame.from_records(results)
        df = pd.concat((df, df_state))

    return df


# data =  extract_data()


