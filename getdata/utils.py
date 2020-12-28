import requests

from .globals import bitcoin, ESPLORA_URL1, ESPLORA_URL2


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


def call_esplora(path):
    try:
        r = requests.get(ESPLORA_URL1 + path)
        if r.ok:
            return r.json()
    except requests.exceptions.ConnectionError:
        pass

    r = requests.get(ESPLORA_URL2 + path)
    r.raise_for_status()
    return r.json()
