from typing import Dict
from tqdm import tqdm
from bitcoin_requests.bitcoin import JSONRPCError

from .globals import bitcoin, last_block
from .onchain import onclose


def inspectblocks(db):
    try:
        with open("last_block") as f:
            blockheight = int(f.read())
    except:
        blockheight = 506425

    end_at_block = last_block

    if blockheight > end_at_block - 14 * 144:
        # if we've reached the end and are up to speed with the blockchain
        # reinspect the last days so we catch closes (and new spends) from new channels
        # we might had not seen in the last scans
        blockheight = end_at_block - 14 * 144

    db.execute("""SELECT short_channel_id, open->>'txid' FROM channels""")
    open_txid_map: Dict[str, str] = {txid: scid for scid, txid in db.fetchall()}

    # go block by block
    with tqdm(total=end_at_block - blockheight) as pbar:
        while blockheight < end_at_block:
            pbar.update()
            pbar.set_description(f"block {blockheight}")

            try:
                block = bitcoin.getblock(bitcoin.getblockhash(blockheight), 2)
                for tx in block["tx"][1:]:  # skip coinbase
                    for vin in tx["vin"]:
                        scid = open_txid_map.get(vin["txid"])
                        if scid and vin["vout"] == int(scid.split("x")[2]):
                            onclose(db, blockheight, block["time"], tx, vin, scid)
            except JSONRPCError as exc:
                print(exc)
                return

            blockheight += 1
            with open("last_block", "w") as f:
                f.write(str(blockheight))
