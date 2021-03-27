import random
import requests

from .globals import bitcoin


def get_fee(tx):
    # multiply stuff by 100000000 because bitcoind returns values in btc
    inputsum = sum(
        [
            int(
                bitcoin.getrawtransaction(inp["txid"], True)["vout"][inp["vout"]][
                    "value"
                ]
                * 100000000
            )
            for inp in tx["vin"]
        ]
    )
    outputsum = sum([int(out["value"] * 100000000) for out in tx["vout"]])

    return inputsum - outputsum


def get_outspends(txid):
    return call_esplora(f"/tx/{txid}/outspends")


esploras = [
    "https://mempool.space/api",
    "https://blockstream.info/api",
    "https://mempool.ninja/api",
    "https://mempool.emzy.de/api",
]


def call_esplora(path):
    random.shuffle(esploras)

    for host in esploras:
        try:
            r = requests.get(host + path)
            if r.ok:
                return r.json()
        except requests.exceptions.ConnectionError:
            pass

    raise Exception("ALL ESPLORAS HAVE FAILED")
