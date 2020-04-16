import datetime
import requests
from tqdm import tqdm
from typing import Dict

from .globals import SPARK_URL, SPARK_TOKEN, bitcoin
from .onchain import onopen


def listchannels(db):
    now = int(datetime.datetime.now().timestamp())

    r = requests.post(
        SPARK_URL, headers={"X-Access": SPARK_TOKEN}, json={"method": "listchannels"}
    )

    db.execute("SELECT short_channel_id, last_update FROM channels")
    channel_last_update_by_scid: Dict[str, int] = {
        scid: int(last_update.timestamp()) for scid, last_update in db.fetchall()
    }

    pbar = tqdm(r.json()["channels"], leave=True, desc="listchannels")
    for ch in pbar:
        pbar.set_description("list " + ch["short_channel_id"])

        if ch["public"] == False:
            continue

        last_update = channel_last_update_by_scid.get(ch["short_channel_id"], 0)

        if not last_update:
            # channel not known, gather onchain data
            blockheight, tx_index, out_n = map(int, ch["short_channel_id"].split("x"))

            # gather onchain data
            block = bitcoin.getblock(bitcoin.getblockhash(blockheight))
            tx = bitcoin.getrawtransaction(block["tx"][tx_index], True)
            onopen(db, blockheight, block["time"], tx, tx["vout"][out_n], ch)

        if last_update < ch["last_update"]:
            # update policies
            save_fee_policies(db, ch)
    pbar.close()

    db.execute("""UPDATE channels SET last_update = to_timestamp(%s)""", (now,))


def save_fee_policies(db, ch):
    node0, node1, towards = (
        (ch["source"], ch["destination"], 1)
        if ch["source"] < ch["destination"]
        else (ch["destination"], ch["source"], 0)
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
