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
        blockheight = 505149

    end_at_block = bitcoin.getblockchaininfo()["blocks"]

    db.execute("""SELECT short_channel_id, open->>'txid' FROM channels""")
    open_txid_map: Dict[str, str] = {txid: scid for scid, txid in db.fetchall()}

    # go block by block
    with tqdm(total=end_at_block - blockheight) as pbar:
        while blockheight < end_at_block:
            pbar.update()
            pbar.set_description(f"inspecting block {blockheight}")

            try:
                inspect_block(db, open_txid_map, blockheight)
            except JSONRPCError:
                return

            blockheight += 1
            with open("last_block", "w") as f:
                f.write(str(blockheight))


def inspect_block(db, open_txid_map, blockheight):
    block = bitcoin.getblock(bitcoin.getblockhash(blockheight), 2)
    for tx in block["tx"][1:]:  # skip coinbase
        for vin in tx["vin"]:
            if scid := open_txid_map.get(vin["txid"]):
                onclose(db, blockheight, block["time"], tx, vin, scid)
