import os
from bitcoin import BitcoinRPC

POSTGRES_URL = os.getenv("POSTGRES_URL")
BITCOIN_RPC_ADDRESS = os.getenv("BITCOIN_RPC_ADDRESS") or "http://127.0.0.1:8443"
BITCOIN_RPC_USER = os.getenv("BITCOIN_RPC_USER")
BITCOIN_RPC_PASSWORD = os.getenv("BITCOIN_RPC_PASSWORD")
SPARK_URL = os.getenv("SPARK_URL")
SPARK_TOKEN = os.getenv("SPARK_TOKEN")
ESPLORA_URL1 = os.getenv("ESPLORA_URL1")
ESPLORA_URL2 = os.getenv("ESPLORA_URL2")

bitcoin = BitcoinRPC(BITCOIN_RPC_ADDRESS, BITCOIN_RPC_USER, BITCOIN_RPC_PASSWORD)

last_block = bitcoin.getblockchaininfo()["blocks"]
