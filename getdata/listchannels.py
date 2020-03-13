import os
import requests

SPARK_URL = os.getenv("SPARK_URL")
SPARK_TOKEN = os.getenv("SPARK_TOKEN")


def listchannels(db):
    r = requests.post(
        SPARK_URL, headers={"X-Access": SPARK_TOKEN}, json={"method": "listchannels"}
    )
    for ch in r.json()["channels"]:
        node0, node1, towards = (
            (ch["source"], ch["destination"], 1)
            if ch["source"] < ch["destination"]
            else (ch["destination"], ch["source"], 0)
        )

        db.execute(
            """
INSERT INTO channels
    (short_channel_id, node0, node1, satoshis, last_update, last_seen)
VALUES (?, ?, ?, ?, ?, datetime('now'))
ON CONFLICT (short_channel_id)
    DO UPDATE SET last_seen = excluded.last_seen, last_update = excluded.last_update
        """,
            (ch["short_channel_id"], node0, node1, ch["satoshis"], ch["last_update"]),
        )

        c = db.execute(
            """
SELECT
  CASE WHEN base_fee_millisatoshi = ? AND fee_per_millionth = ? AND delay = ?
    THEN 1
    ELSE 0
  END
FROM policies
WHERE short_channel_id = ? AND direction = ?
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
        row = c.fetchone()
        isfeepolicyuptodate = row[0] if row else False

        if not isfeepolicyuptodate:
            db.execute(
                """
INSERT INTO policies
    (short_channel_id, direction,
     base_fee_millisatoshi, fee_per_millionth, delay,
     update_time)
VALUES (?, ?, ?, ?, ?, ?)
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
