import pandas as pd 
import numpy as np 
import json
# from typing import Dict
# from dataclasses import dataclass

# from websocket import create_connection
# import websocket, ssl


# @dataclass
# class Ticker:
#     product_id : str
#     ts_unix : int
#     price : float
#     size : float


# PRODUCT_IDS = [
#     # "BTC-USD",
#     "ETH-USD",
#     # "SOL-USD"
# ]


# ws = ws = create_connection("wss://ws-feed.exchange.coinbase.com",sslopt={"cert_reqs": ssl.CERT_NONE})

# ws.send(
#     json.dumps(
#         {
#             "type": "subscribe",
#             "product_ids": ["ETH-USD"],
#             "channels": ["ticker"],
#         }
#     )
# )
# # The first msg is just a confirmation that we have subscribed.
# for i in range(10):
#     print(ws.recv())

# ws.close()


# data = pd.read_csv("D:\Grind\DataEngineering\Datatalks\CoinbasePipeline\otebooks\estat_lfsa_qoe_3a4.tsv",sep="\t")
# print(data.shape)
# print(data.head(4))



