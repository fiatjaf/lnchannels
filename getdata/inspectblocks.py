from typing import Dict
from tqdm import tqdm
from bitcoin.bitcoin import JSONRPCError

from .globals import bitcoin
from .onchain import onclose


def inspectblocks(db):
    try:
        with open("last_block") as f:
            blockheight = int(f.read())
    except:
        blockheight = 506425

    end_at_block = bitcoin.getblockchaininfo()["blocks"]

    if blockheight > end_at_block - 7 * 144:
        # if we've reached the end and are up to speed with the blockchain
        # reinspect the last 7 days so we catch closes from new channels
        # we might had not seen in the last (7?) scans
        blockheight = end_at_block - 7 * 144

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
            except JSONRPCError:
                return

            blockheight += 1
            with open("last_block", "w") as f:
                f.write(str(blockheight))
