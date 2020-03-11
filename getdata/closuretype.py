import os
import requests
from bitcoin import BitcoinRPC

BITCOIN_RPC_ADDRESS = os.getenv("BITCOIN_RPC_ADDRESS") or "http://127.0.0.1:8443"
BITCOIN_RPC_USER = os.getenv("BITCOIN_RPC_USER")
BITCOIN_RPC_PASSWORD = os.getenv("BITCOIN_RPC_PASSWORD")

bitcoin = BitcoinRPC(BITCOIN_RPC_ADDRESS, BITCOIN_RPC_USER, BITCOIN_RPC_PASSWORD)


def closuretype(db):
    c = db.execute(
        "SELECT short_channel_id, close_transaction FROM channels WHERE close_type IS NULL AND close_time IS NOT NULL and close_time < datetime('now', '-7 day')"
    )
    for row in c:
        scid, close_txid = row

        # defaults
        typ = "unknown"
        bal_a = 0
        bal_b = 0
        nhtlcs = 0

        close = bitcoin.getrawtransaction(close_txid, True)
        outs = []

        # label each output of the closing transaction
        # (we'll have to look at the transactions that spend them)
        for out in close["vout"]:
            if out["script_pub_key"]["typ"] == "witness_v0_keyhash":
                outs.append("pubkey")
            else:
                address = out["script_pub_key"]["addresses"][0]
                r = requests.get(f"https://blockstream.info/api/address/{address}/txs")
                r.raise_for_status()

                # find the followup transaction and the witness data we need
                # to determine the type of the previous
                witness = None
                for followuptx in r.json():
                    f = bitcoin.getrawtransaction(followuptx["txid"])
                    # find the output we're interested in
                    # if the followup transaction uses many
                    for inp in f["vin"]:
                        if inp["txid"] == close_txid and inp["vout"] == out["n"]:
                            witness = inp["txinwitness"]
                            break
                    if witness:
                        break

                if not witness:
                    # didn't find a witness.
                    # transaction wasn't spent (and also isn't a pubkey). very odd.
                    outs.push("unknown")
                    continue

                script = bitcoin.decodescript(witness[-1])
                if "OP_HASH160" in script:
                    outs.append("htlc")
                elif "OP_CHECKSEQUENCEVERIFY" in script:
                    if witness[-2] == "01":
                        outs.append("penalty")
                    else:
                        outs.append("balance")
                else:
                    outs.append("unknown")

        # now that we have labels for all outputs we use a simple (maybe wrong?)
        # heuristic to determine what happened.
        if len(outs) == 1 and outs[0] == "pubkey":
            typ = "unused"
            bal_a = close["vout"][0]["value"] * 100000000
        elif len(outs) == 2 and outs[0] == "pubkey" and outs[1] == "pubkey":
            typ = "mutual"
            bal_a = close["vout"][0]["value"] * 100000000
            bal_a = close["vout"][1]["value"] * 100000000
        else:
            i = 0
            for out in outs:
                if out == "htlc":
                    nhtlcs += 1
                    continue

                if out == "penalty":
                    typ = "penalty"

                if out == "balance":
                    typ = "force"

                if out == "penalty" or out == "balance" or out == "pubkey":
                    if bal_a == 0:
                        bal_a = close["vout"][i]["value"] * 100000000
                    elif bal_b == 0:
                        bal_b = close["vout"][i]["value"] * 100000000
                    else:
                        # this should never happen
                        typ = "unknown"

                i += 1

        # no matter how balances were arranged above
        # if we detected a penalty we can assume everything is on one side
        if typ == "penalty":
            bal_a += bal_b
            bal_b = 0

        db.execute(
            """
UPDATE channels
  SET
    close_type = ?,
    close_balance_a = ?,
    close_balance_b = ?,
    close_htlc_count = ?
WHERE short_channel_id = ?
        """,
            (typ, bal_a, bal_b, nhtlcs, scid),
        )
