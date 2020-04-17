import json
from typing import Dict

from .utils import get_fee, get_outspends
from .globals import bitcoin


def onopen(db, blockheight: int, blocktime: int, tx: Dict, vout: Dict, ch: Dict):
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
        ch["last_update"],
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
  INSERT INTO channels (short_channel_id, nodes, satoshis, last_update)
  VALUES (%s, %s, %s, to_timestamp(%s))
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

            kinds.add("unknown")  # default
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
