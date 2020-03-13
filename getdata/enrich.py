import os
from bitcoin import BitcoinRPC

BITCOIN_RPC_ADDRESS = os.getenv("BITCOIN_RPC_ADDRESS") or "http://127.0.0.1:8443"
BITCOIN_RPC_USER = os.getenv("BITCOIN_RPC_USER")
BITCOIN_RPC_PASSWORD = os.getenv("BITCOIN_RPC_PASSWORD")

bitcoin = BitcoinRPC(BITCOIN_RPC_ADDRESS, BITCOIN_RPC_USER, BITCOIN_RPC_PASSWORD)


def enrich(db):
    c = db.execute("SELECT short_channel_id FROM channels WHERE open_block IS NULL")
    for row in c:
        scid = row[0]
        blockheight, txindex, output = [int(part) for part in scid.split("x")]
        block = bitcoin.getblock(bitcoin.getblockhash(blockheight))
        txid = block["tx"][txindex]
        tx = bitcoin.getrawtransaction(txid, True)
        address = tx["vout"][output]["scriptPubKey"]["addresses"][0]
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
        fee = inputsum - outputsum
        db.execute(
            """
UPDATE channels
SET open_block = ?, open_transaction = ?, address = ?, open_time = ?, open_fee = ?
WHERE short_channel_id = ?
        """,
            (blockheight, txid, address, block["time"], fee, scid),
        )
        print("enriched", scid)
