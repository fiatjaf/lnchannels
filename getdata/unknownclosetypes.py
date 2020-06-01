from tqdm import tqdm

from .globals import bitcoin
from .onchain import onclose


def unknownclosetypes(db):
    db.execute(
        """
        SELECT short_channel_id, close->>'txid', (close->>'block')::int, close->>'time'
        FROM channels
        WHERE close->>'block' IS NOT NULL
          AND close->>'type' = 'unknown'
    """
    )

    rows = db.fetchall()

    with tqdm(total=len(rows)) as pbar:
        for scid, txid, blockheight, time in rows:
            pbar.update()
            pbar.set_description(f"unknown close type {scid}")
            tx = bitcoin.getrawtransaction(txid, True)
            vin = filter(lambda vin: vin["vout"] == int(scid.split("x")[2]), tx["vin"])
            onclose(db, blockheight, time, tx, vin, scid)
