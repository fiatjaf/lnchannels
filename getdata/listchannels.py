import os
import random
import requests
from bitcoin import BitcoinRPC

BITCOIN_RPC_ADDRESS = os.getenv("BITCOIN_RPC_ADDRESS") or "http://127.0.0.1:8443"
BITCOIN_RPC_USER = os.getenv("BITCOIN_RPC_USER")
BITCOIN_RPC_PASSWORD = os.getenv("BITCOIN_RPC_PASSWORD")
SPARK_URL = os.getenv("SPARK_URL")
SPARK_TOKEN = os.getenv("SPARK_TOKEN")

bitcoin = BitcoinRPC(BITCOIN_RPC_ADDRESS, BITCOIN_RPC_USER, BITCOIN_RPC_PASSWORD)


def listchannels(db):
    info = bitcoin.getblockchaininfo()
    tip = info["blocks"]
    since = tip - int(144 * 30 * random.random())

    r = requests.post(
        SPARK_URL, headers={"X-Access": SPARK_TOKEN}, json={"method": "listchannels"}
    )
    for ch in r.json()["channels"]:
        block, *_ = ch["short_channel_id"].split("x")
        if int(block) < since:
            continue

        node0, node1, towards = (
            (ch["source"], ch["destination"], 1)
            if ch["source"] < ch["destination"]
            else (ch["destination"], ch["source"], 0)
        )

        db.execute(
            """
INSERT INTO channels
    (short_channel_id, node0, node1, satoshis, last_seen)
VALUES (%s, %s, %s, %s, now())
ON CONFLICT (short_channel_id)
    DO UPDATE SET last_seen = excluded.last_seen
        """,
            (ch["short_channel_id"], node0, node1, ch["satoshis"]),
        )

        db.execute(
            """
SELECT
  CASE WHEN base_fee_millisatoshi = %s AND fee_per_millionth = %s AND delay = %s
    THEN 1
    ELSE 0
  END
FROM policies
WHERE short_channel_id = %s AND direction = %s
ORDER BY update_time DESC
LIMIT 1
        """,
            (
                ch["base_fee_millisatoshi"],
                ch["fee_per_millionth"],
                ch["delay"],
                ch["short_channel_id"],
                towards,
            ),
        )
        row = db.fetchone()
        isfeepolicyuptodate = row[0] if row else False

        if not isfeepolicyuptodate:
            db.execute(
                """
INSERT INTO policies
    (short_channel_id, direction,
     base_fee_millisatoshi, fee_per_millionth, delay,
     update_time)
VALUES (%s, %s, %s, %s, %s, to_timestamp(%s))
            """,
                (
                    ch["short_channel_id"],
                    towards,
                    ch["base_fee_millisatoshi"],
                    ch["fee_per_millionth"],
                    ch["delay"],
                    ch["last_update"],
                ),
            )

        print("inserted", ch["short_channel_id"])
