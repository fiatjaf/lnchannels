import json
from typing import Dict

from .utils import get_fee, get_outspends
from .globals import bitcoin, last_block


def onopen(
    db,
    blockheight: int,
    blocktime: int,
    tx: Dict,
    vout: Dict,
    short_channel_id: str,
    ch: Dict,
):
    open_data = {
        "block": blockheight,
        "txid": tx["txid"],
        "address": vout["scriptPubKey"]["addresses"][0],
        "time": blocktime,
        "fee": get_fee(tx),
    }

    txs_funding = set.union({v["txid"] for v in tx["vin"]},)
    txs = {"funding": list(txs_funding)}

    if ch:
        node0, node1, towards = (
            (ch["source"], ch["destination"], 1)
            if ch["source"] < ch["destination"]
            else (ch["destination"], ch["source"], 0)
        )

        db.execute(
            """
INSERT INTO channels (short_channel_id, nodes, satoshis, last_update)
VALUES (%s, %s, %s, to_timestamp(%s))
ON CONFLICT (short_channel_id) DO NOTHING
        """,
            (
                short_channel_id,
                json.dumps([node0, node1]),
                ch["satoshis"],
                ch["last_update"],
            ),
        )

    db.execute(
        """
UPDATE channels
SET open = %s
  , txs = txs || %s
WHERE short_channel_id = %s
        """,
        (json.dumps(open_data), json.dumps(txs), short_channel_id),
    )


def onclose(db, blockheight, blocktime, tx, vin, scid):
    txs = {"a": set(), "b": set()}
    spends = get_outspends(tx["txid"])
    kinds = set()
    htlc_list = []
    balance = {"a": 0, "b": 0}
    htlcs = []
    closer = None
    close_type = "unknown"

    # inspect each output of the closing transaction
    # (we'll have to look at the transactions that spend them)
    next_side = "a"  # the first 'balance' or 'any' output is 'a', the second is 'b'
    for i, spend in enumerate(spends):
        amount = int(tx["vout"][i]["value"] * 100000000)
        side = next_side

        if spend["spent"] == False or not spend["status"]["confirmed"]:
            # we can't know what this is, maybe it's a mutual closure and the
            # funds are waiting at someone's wallet, or it's a delayed output
            # that wasn't spent yet because the time hasn't arrived
            if blockheight + 3000 > last_block:
                kinds.add("unknown")
            else:
                kinds.add("any")

            next_side = "b"
            balance[side] = amount
        else:
            f = bitcoin.getrawtransaction(spend["txid"], True)
            witness = f["vin"][spend["vin"]]["txinwitness"]

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
                        if next_spend["spent"] and next_spend["status"]["confirmed"]:
                            txs[side].add(next_spend["txid"])

                    # in case of a delayed output, we know this was the force-closer
                    # and also the side that got taken
                    closer = side

                    if witness[-2] == "01":
                        kinds.add("penalty")
                        # we keep treating this as if 'a' and 'b' were different
                        # but we'll know later they're the same node.
                        # we can't ever know who was the "closer" here
                        # as both 'a' and 'b' outputs will go to the same peer.
                    else:
                        kinds.add("delayed")

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
                # spend here refer to the transaction that spends the
                # transaction that is spending the htlc (htlc+2), so witness/script
                # are relative to the transaction that spends the htlc (htlc+1)
                spend = spends[htlc["vout"]]
                if not spend["spent"] or not spend["status"]["confirmed"]:
                    raise IndexError

                f = bitcoin.getrawtransaction(spend["txid"], True)
                witness = f["vin"][spend["vin"]]["txinwitness"]
                script = bitcoin.decodescript(witness[-1])["asm"]

                # if the node closing the channel is trying to spend
                # the htlc, the htlc+1 tx must have an OP_CHECKSEQUENCEVERIFY
                # (which we are calling the "covenant") in it, which we
                # should be able to see here in the script we got from htlc+2
                if "OP_CHECKSEQUENCEVERIFY" in script:
                    has_covenant = True
                    for s in get_outspends(spend["txid"]):
                        if s["spent"] and s["status"]["confirmed"]:
                            # this is for future chainanalysis using this tx
                            txs[closer].add(s["txid"])
                else:
                    raise IndexError
            except IndexError:
                # should fall here when there's no covenant
                has_covenant = False
                for s in spends:
                    if s["spent"]:
                        # this is for future chainanalysis using this tx
                        txs[noncloser].add(s["txid"])

            # now we determine if the htlc was fulfilled or not and to whom
            if "OP_NOTIF" in htlc["script"]:
                # OP_NOTIF indicates it the closer who offered, we don't care about
                # the meaning, it's just a naïve text matching, see templates at:
                # https://github.com/lightningnetwork/lightning-rfc/blob/master/03-transactions.md#offered-htlc-outputs
                offerer = closer

                # when the htlc is spent with a covenant it means the closer got it.
                # here the receiver is noncloser.
                if has_covenant:
                    fulfilled = False
                else:
                    fulfilled = True
            else:
                # (reverse of the above)
                offerer = noncloser
                # here the receiver is the closer.
                if has_covenant:
                    fulfilled = True
                else:
                    fulfilled = False

            htlcs.append(
                {"amount": htlc["amount"], "offerer": offerer, "fulfilled": fulfilled}
            )

    db.execute(
        """
UPDATE channels
SET close = %s
  , txs = txs || %s
  , closer = %s
WHERE short_channel_id = %s
    """,
        (
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
            closer,
            scid,
        ),
    )
