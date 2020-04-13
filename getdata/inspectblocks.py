import re
import json
import requests
from typing import Dict
from tqdm import tqdm
from bitcoin.bitcoin import JSONRPCError

from .globals import SPARK_URL, SPARK_TOKEN, bitcoin
from .utils import get_fee, get_outspends


def inspectblocks(db):
    try:
        with open("last_block") as f:
            blockheight = int(f.read())
    except:
        blockheight = 505149

    channels_by_scid: Dict[str, Dict] = {
        ch["short_channel_id"]: ch
        for ch in requests.post(
            SPARK_URL,
            headers={"X-Access": SPARK_TOKEN},
            json={"method": "listchannels"},
        ).json()["channels"]
        if ch["public"]
    }

    db.execute("""SELECT short_channel_id, onchain->'open'->>'txid' FROM channels""")
    open_txid_map: Dict[str, str] = {row[0]: row[1] for row in db.fetchall()}

    # revisit past stuff that may be channels
    db.execute(
        """
SELECT
  short_channel_id,
  split_part(short_channel_id, 'x', 1)::int,
  split_part(short_channel_id, 'x', 2)::int,
  txid
FROM try_later
WHERE txid IS NOT NULL AND tries < 7
    """
    )
    for scid, blockheight, tx_index, txid in tqdm(
        db.fetchall(), desc="try_later", leave=True
    ):
        tx = bitcoin.getrawtransaction(txid, True)
        inspect_tx(
            db,
            channels_by_scid,
            open_txid_map,
            blockheight,
            tx["blocktime"],
            tx_index,
            tx,
        )

    # go block by block
    while True:
        try:
            inspect_block(db, channels_by_scid, open_txid_map, blockheight)
        except JSONRPCError:
            return

        blockheight += 1
        with open("last_block", "w") as f:
            f.write(str(blockheight))


def inspect_block(db, channels_by_scid, open_txid_map, blockheight):
    block = bitcoin.getblock(bitcoin.getblockhash(blockheight), 2)
    for i, tx in tqdm(
        enumerate(block["tx"]),
        desc=f"block {blockheight}",
        leave=True,
        total=block["nTx"],
    ):
        inspect_tx(
            db, channels_by_scid, open_txid_map, blockheight, block["time"], i, tx
        )


def inspect_tx(
    db, channels_by_scid, open_txid_map, blockheight, blocktime, tx_index, tx
):
    # check channel closes
    for vin in tx["vin"]:
        try:
            witness = vin["txinwitness"]
        except KeyError:
            # not segwit
            continue

        script = bitcoin.decodescript(witness[-1])["asm"]
        if re.match(r"2 \w+ \w+ 2 OP_CHECKMULTISIG", script):
            # maybe a channel closure
            if not vin["txid"] in open_txid_map:
                continue

            scid = open_txid_map[vin["txid"]]
            print(f"  {scid} channel closed!")
            onclose(db, blockheight, blocktime, tx, vin, scid)

            # assume there won't be a channel close and open in the same tx
            # or two closes
            return

    # check channel opens
    for vout in tx["vout"]:
        if vout["scriptPubKey"]["type"] != "witness_v0_scripthash":
            # not a channel
            continue

        # maybe a channel open
        scid = f"{blockheight}x{tx_index}x{vout['n']}"
        if scid not in channels_by_scid:
            # channel not found online
            # see if we have it
            if tx["txid"] in open_txid_map:
                pass
            else:
                # must be checked again later TODO
                db.execute(
                    """
INSERT INTO try_later (short_channel_id, txid) VALUES (%s, %s)
ON CONFLICT (short_channel_id) DO UPDATE SET tries = try_later.tries + 1
                    """,
                    (scid, tx["txid"]),
                )
            continue

        ch = channels_by_scid[scid]
        print(f"  {scid} channel opened!")
        onopen(db, blockheight, blocktime, tx, vout, ch)


def onopen(db, blockheight, blocktime, tx, vout, ch):
    node0, node1, towards = (
        (ch["source"], ch["destination"], 1)
        if ch["source"] < ch["destination"]
        else (ch["destination"], ch["source"], 0)
    )

    txs_funding = set.union(
        # txs that contributed inputs for the channel funding
        {v["txid"] for v in tx["vin"]},
        # TODO change outputs from the channel creation
        # (must revisit this later to add more stuff?)
        # {
        #     s["txid"]
        #     for i, s in enumerate(get_outspends(tx["txid"]))
        #     if i != vout["n"] and s["spent"]
        # },
    )

    params = (
        ch["short_channel_id"],
        json.dumps([node0, node1]),
        ch["satoshis"],
        json.dumps(
            {
                "block": blockheight,
                "txid": tx["txid"],
                "address": vout["scriptPubKey"]["addresses"][0],
                "time": blocktime,
                "fee": get_fee(tx),
            }
        ),
        json.dumps({"funding": list(txs_funding)}),
        ch["short_channel_id"],
    )
    print(params)
    db.execute(
        """
WITH ins AS (
  INSERT INTO channels (short_channel_id, nodes, satoshis)
  VALUES (%s, %s, %s)
  ON CONFLICT (short_channel_id) DO NOTHING
)
UPDATE channels
SET open = %s
  , txs = txs || %s
WHERE short_channel_id = %s
        """,
        params,
    )


def onclose(db, blockheight, blocktime, tx, vin, scid):
    txs = {"a": set(), "b": set()}
    spends = get_outspends(tx["txid"])
    kinds = set()
    htlc_list = []
    balance = {"a": 0, "b": 0}
    htlcs = []
    taken = None
    closer = None
    close_type = "unknown"

    # inspect each output of the closing transaction
    # (we'll have to look at the transactions that spend them)
    next_side = "a"  # the first 'balance' or 'any' output is 'a', the second is 'b'
    for i, spend in enumerate(spends):
        amount = int(tx["vout"][i]["value"] * 100000000)
        side = next_side

        if spend["spent"] == False:
            # big indicator of mutual closure
            kinds.add("any")
            next_side = "b"
            balance[side] = amount
        else:
            f = bitcoin.getrawtransaction(spend["txid"], True)
            witness = f["vin"][spend["vin"]]["txinwitness"]
            kinds.add("unknown")
            script = None

            if len(witness) == 2:
                # paying to a pubkey
                kinds.add("any")
                next_side = "b"
                balance[side] = amount
                txs[side].add(spend["txid"])
            else:
                script = bitcoin.decodescript(witness[-1])["asm"]
                if "OP_HASH160" in script:
                    kinds.add("htlc")
                    htlc_list.append(
                        {
                            "script": script,
                            "amount": amount,
                            "txid": spend["txid"],
                            "vout": i,
                        }
                    )
                elif "OP_CHECKSEQUENCEVERIFY" in script:
                    balance[side] = amount
                    next_side = "b"

                    for next_spend in get_outspends(spend["txid"]):
                        if next_spend["spent"]:
                            txs[side].add(next_spend["txid"])

                    if witness[-2] == "01":
                        kinds.add("penalty")
                        # we keep treating this as if 'a' and 'b' were different
                        # but we'll know later they're the same node.
                        # we can't ever know who was the "closer" here
                        # as both 'a' and 'b' outputs will go to the same peer.

                        # however we do mark the side that was appropriated
                        taken = side
                    else:
                        kinds.add("delayed")

                        # in case of a delayed output, we know this was the force-closer
                        closer = side
                else:
                    # paying to a custom address, means the same as a pubkey
                    # (it's anything the peer wants to spend to either in a
                    #  mutual close or when the other party is force-closing)
                    kinds.add("any")
                    balance[side] = amount
                    txs[side].add(spend["txid"])
                    next_side = "b"

    # now that we have kinds of all outputs we can determine the closure type
    if kinds == {"any"}:
        close_type = "mutual"
    elif "penalty" in kinds:
        close_type = "penalty"
    elif "htlc" in kinds or "delayed" in kinds:
        close_type = "force"
    else:
        close_type = "unknown"

    # in case of htlcs, we want to know to each side they went
    if closer:
        closer, noncloser = (
            closer,
            "b" if closer == "a" else "a",
        )
        for htlc in htlc_list:
            # first we check if there's an htlc-success or htlc-timeout spending this
            spends = get_outspends(htlc["txid"])

            try:
                spend = spends[htlc["vout"]]
                if not spend["spent"]:
                    raise IndexError

                f = bitcoin.getrawtransaction(spend["txid"], True)
                witness = f["vin"][spend["vin"]]["txinwitness"]
                script = bitcoin.decodescript(witness[-1])["asm"]
                if "OP_CHECKSEQUENCEVERIFY" in script:
                    has_covenant = True
                    for s in get_outspends(spend["txid"]):
                        if s["spent"]:
                            txs[closer].add(s["txid"])
                else:
                    raise IndexError
            except IndexError:
                # should fall here when there's no covenant
                has_covenant = False
                for s in spends:
                    if s["spent"]:
                        txs[noncloser].add(s["txid"])

            htlcs.append(
                {
                    "amount": htlc["amount"],
                    # OP_NOTIF indicates it was offered to the closer
                    "offerer": noncloser if "OP_NOTIF" in htlc["script"] else closer,
                    # when the htlc is spent with a covenant it means the closer got it
                    "fulfilled": True
                    if (
                        ("OP_NOTIF" in htlc["script"] and has_covenant)
                        or ("OP_NOTIF" not in htlc["script"] and not has_covenant)
                    )
                    else False,
                }
            )

    params = (
        json.dumps(
            {
                "block": blockheight,
                "txid": tx["txid"],
                "time": blocktime,
                "fee": get_fee(tx),
                "type": close_type,
                "balance": balance,
                "htlcs": htlcs,
            }
        ),
        json.dumps({"a": list(txs["a"]), "b": list(txs["b"])}),
        taken,
        closer,
        scid,
    )
    print(params)
    db.execute(
        """
UPDATE channels
SET close = %s
  , txs = txs || %s
  , taken = %s
  , closer = %s
WHERE short_channel_id = %s
    """,
        params,
    )
