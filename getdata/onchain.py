import json
import requests

from .globals import ESPLORA, bitcoin


class OnchainError(Exception):
    pass


class ChannelStillOpen(OnchainError):
    pass


class OutputNotSpentYet(OnchainError):
    pass


def onchain(db):
    print("")

    db.execute(
        """
SELECT short_channel_id, onchain
FROM channels
WHERE onchain IS NULL
  OR ( (onchain->'close'->'block' IS NULL OR onchain->'close'->>'block' IS NULL)
   AND last_seen < now() - '1 day'::interval
  )
  OR ( (onchain->'close'->'type' IS NULL OR onchain->'close'->>'type' IS NULL)
   AND last_seen < now() - '7 days'::interval
  )
ORDER BY short_channel_id
""",
    )

    for scid, data in db.fetchall():
        onchain_for_scid(db, scid, data)


def onchain_for_scid(db, scid, data):
    # get data for all possibly related channels
    print("  onchain", scid)
    data.setdefault("a", None)
    data.setdefault("b", None)
    data.setdefault("closer", None)
    data.setdefault("funder", None)
    data.setdefault(
        "open",
        {"block": None, "txid": None, "time": None, "fee": None, "address": None},
    )
    data.setdefault(
        "close",
        {
            "block": None,
            "txid": None,
            "time": None,
            "fee": None,
            "balance": {"a": 0, "b": 0},
            "htlcs": {"a": [], "b": []},
        },
    )
    data.setdefault("txs", {"a": [], "b": [], "funding": []})

    try:
        onopen(scid, data)
        isclosed(scid, data)
        onclose(data)
    except OutputNotSpentYet:
        pass
    except ChannelStillOpen:
        pass
    except OnchainError as exc:
        print(exc)
        return

    db.execute(
        """
UPDATE channels
SET onchain = %s
WHERE short_channel_id = %s
           """,
        (json.dumps(data), scid),
    )


def onopen(scid, data):
    if data["open"].get("block"):
        return

    # basic onchain data
    blockheight, txindex, output = [int(part) for part in scid.split("x")]
    block = bitcoin.getblock(bitcoin.getblockhash(blockheight))
    txid = block["tx"][txindex]
    tx = bitcoin.getrawtransaction(txid, True)
    address = tx["vout"][output]["scriptPubKey"]["addresses"][0]

    data["open"] = {
        "block": blockheight,
        "txid": txid,
        "address": address,
        "time": tx["blocktime"],
        "fee": get_fee(tx),
    }
    data["txs"]["funding"] += [v["txid"] for v in tx["vin"]]


def isclosed(scid, data):
    if data["close"].get("block"):
        return

    chan_vout = int(scid.split("x")[2])
    spends = get_outspends(data["open"]["txid"])
    chan_spend = spends[chan_vout]
    if not chan_spend["spent"] or "block_height" not in chan_spend["status"]:
        raise ChannelStillOpen

    close_txid = chan_spend["txid"]
    close_tx = bitcoin.getrawtransaction(close_txid, True)

    data["close"].update(
        {
            "block": chan_spend["status"]["block_height"],
            "txid": close_txid,
            "time": chan_spend["status"]["block_time"],
            "fee": get_fee(close_tx),
        }
    )
    data["txs"]["funding"] += [
        # change outputs from this channel creation
        s["txid"]
        for i, s in enumerate(spends)
        if i != chan_vout and s["spent"]
    ]


def onclose(data):
    if data["close"].get("type"):
        return

    txs = {"a": set(), "b": set()}

    close_tx = bitcoin.getrawtransaction(data["close"]["txid"], True)
    spends = get_outspends(data["close"]["txid"])
    kinds = set()
    htlcs = []

    # inspect each output of the closing transaction
    # (we'll have to look at the transactions that spend them)
    next_side = "a"  # the first 'balance' or 'any' output is 'a', the second is 'b'
    for i, spend in enumerate(spends):
        amount = int(close_tx["vout"][i]["value"] * 100000000)
        side = next_side

        if spend["spent"] == False:
            # big indicator of mutual closure
            kinds.add("any")
            next_side = "b"
            data["close"]["balance"][side] = amount
        else:
            f = bitcoin.getrawtransaction(spend["txid"], True)
            witness = f["vin"][spend["vin"]]["txinwitness"]
            kinds.add("unknown")
            script = None

            if len(witness) == 2:
                # paying to a pubkey
                kinds.add("any")
                next_side = "b"
                data["close"]["balance"][side] = amount
                txs[side].add(spend["txid"])
            else:
                script = bitcoin.decodescript(witness[-1])["asm"]
                if "OP_HASH160" in script:
                    kinds.add("htlc")
                    htlcs.append(
                        {
                            "script": script,
                            "amount": amount,
                            "txid": spend["txid"],
                            "vout": i,
                        }
                    )
                elif "OP_CHECKSEQUENCEVERIFY" in script:
                    data["close"]["balance"][side] = amount
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
                        data["taken"] = side
                    else:
                        kinds.add("delayed")

                        # in case of a delayed output, we know this was the force-closer
                        data["closer"] = side
                else:
                    # paying to a custom address, means the same as a pubkey
                    # (it's anything the peer wants to spend to either in a
                    #  mutual close or when the other party is force-closing)
                    kinds.add("any")
                    data["close"]["balance"][side] = amount
                    txs[side].add(spend["txid"])
                    next_side = "b"

    # now that we have kinds of all outputs we can determine the closure type
    if kinds == {"any"}:
        data["close"]["type"] = "mutual"
    elif "penalty" in kinds:
        data["close"]["type"] = "penalty"
    elif "htlc" in kinds or "delayed" in kinds:
        data["close"]["type"] = "force"
    else:
        data["close"]["type"] = "unknown"

    # in case of htlcs, we want to know to each side they went
    if data["closer"]:
        closer, noncloser = (
            data["closer"],
            "b" if data["closer"] == "a" else "a",
        )
        for htlc in htlcs:
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

            amt = htlc["amount"]

            if "OP_NOTIF" in htlc["script"]:
                # from the point of view of closer, this is an offered htlc
                if has_covenant:
                    # it goes to the closer if there is an htlc-timeout tx
                    data["close"]["htlcs"][closer].append(
                        {"amount": amt, "kind": "timeout"}
                    )
                else:
                    # otherwise goes to the noncloser (spending from here directly)
                    data["close"]["htlcs"][noncloser].append(
                        {"amount": amt, "kind": "success"}
                    )
            else:
                # from the point of view of closer, this is a received htlc
                if has_covenant:
                    # it goes to the closer if there is an htlc-success tx
                    data["close"]["htlcs"][closer].append(
                        {"amount": amt, "kind": "success"}
                    )
                else:
                    # otherwise goes to the noncloser (spending from here directly)
                    data["close"]["htlcs"][noncloser].append(
                        {"amount": amt, "kind": "timeout"}
                    )

    data["txs"]["a"] = list(txs["a"])
    data["txs"]["b"] = list(txs["b"])


def get_outspends(txid):
    return call_esplora(f"/tx/{txid}/outspends")


def call_esplora(path):
    try:
        r = requests.get(f"{ESPLORA}/{path}")
    except requests.exceptions.ConnectionError:
        raise OnchainError()
    if not r.ok:
        raise OnchainError()
    return r.json()


def get_fee(tx):
    # multiply stuff by 100000000 because bitcoind returns values in btc
    inputsum = sum(
        [
            int(
                bitcoin.getrawtransaction(inp["txid"], True)["vout"][inp["vout"]][
                    "value"
                ]
                * 100000000
            )
            for inp in tx["vin"]
        ]
    )
    outputsum = sum([int(out["value"] * 100000000) for out in tx["vout"]])

    return inputsum - outputsum
