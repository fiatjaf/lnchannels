import requests


def checkcloses(db):
    db.execute(
        "SELECT short_channel_id, address FROM channels WHERE close_block IS NULL and last_seen < (now() - '1 day'::interval)"
    )
    for row in c:
        scid, address = row
        try:
            r = requests.get(f"https://blockstream.info/api/address/{address}/txs")
        except requests.exceptions.ConnectionError:
            continue
        if not r.ok:
            continue

        txs = r.json()
        if len(txs) == 2 and txs[0]["status"]["confirmed"]:
            print(scid, "closed")

            # don't multiply by 100000000 here because esplora returns values in sat
            outputsum = sum([out["value"] for out in txs[0]["vout"]])
            inputsum = sum([inp["prevout"]["value"] for inp in txs[0]["vin"]])
            fee = inputsum - outputsum
            db.execute(
                """
UPDATE channels
SET close_block = %s, close_transaction = %s, close_time = %s, close_fee = %s
WHERE short_channel_id = %s
            """,
                (
                    txs[0]["status"]["block_height"],
                    txs[0]["txid"],
                    txs[0]["status"]["block_time"],
                    fee,
                    scid,
                ),
            )
        else:
            print(scid, "not closed")
