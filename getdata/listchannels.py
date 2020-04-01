import json
import random
import requests

from .globals import SPARK_URL, SPARK_TOKEN, bitcoin


def listchannels(db):
    info = bitcoin.getblockchaininfo()
    tip = info["blocks"]
    since = tip - int(144 * 60 * random.random())

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
    (short_channel_id, nodes, satoshis, last_seen)
VALUES (%s, %s, %s, %s, now())
ON CONFLICT (short_channel_id)
    DO UPDATE SET last_seen = excluded.last_seen
        """,
            (ch["short_channel_id"], json.dumps([node0, node1]), ch["satoshis"]),
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
