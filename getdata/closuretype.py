import os
import requests
from bitcoin import BitcoinRPC

BITCOIN_RPC_ADDRESS = os.getenv("BITCOIN_RPC_ADDRESS") or "http://127.0.0.1:8443"
BITCOIN_RPC_USER = os.getenv("BITCOIN_RPC_USER")
BITCOIN_RPC_PASSWORD = os.getenv("BITCOIN_RPC_PASSWORD")

bitcoin = BitcoinRPC(BITCOIN_RPC_ADDRESS, BITCOIN_RPC_USER, BITCOIN_RPC_PASSWORD)


def closuretypes(db):
    db.execute(
        "SELECT short_channel_id, close_transaction FROM channels WHERE close_type IS NULL AND close_time IS NOT NULL and close_time < (now() - '7 days'::interval)"
    )
    for row in db.fetchall():
        scid, close_txid = row

        print(scid, end=" ")

        try:
            typ, bal_a, bal_b, nhtlcs = closuretype(scid, close_txid)
        except ClosureTypeError as exc:
            print("couldn't determine:", exc)
            continue

        db.execute(
            """
UPDATE channels
  SET
    close_type = %s,
    close_balance_a = %s,
    close_balance_b = %s,
    close_htlc_count = %s
WHERE short_channel_id = %s
        """,
            (typ, bal_a, bal_b, nhtlcs, scid),
        )

        print("\t", typ, "with", nhtlcs, "htlcs")


class ClosureTypeError(Exception):
    pass


def closuretype(scid, close_txid):
    # defaults
    typ = "unknown"
    bal_a = 0
    bal_b = 0
    nhtlcs = 0

    try:
        r = requests.get(f"https://blockstream.info/api/tx/{close_txid}/outspends")
    except requests.exceptions.ConnectionError:
        raise ClosureTypeError()
    if not r.ok:
        raise ClosureTypeError()

    spends = r.json()
    outs = []

    # label each output of the closing transaction
    # (we'll have to look at the transactions that spend them)
    for spend in spends:
        if spend["spent"] == False:
            raise ClosureTypeError("not spent yet")

        f = bitcoin.getrawtransaction(spend["txid"], True)
        witness = f["vin"][spend["vin"]]["txinwitness"]
        amount = sum([int(vout["value"] * 10000000) for vout in f["vout"]])

        if len(witness) == 2:
            # paying to a pubkey
            outs.append(("any", amount))
        else:
            script = bitcoin.decodescript(witness[-1])["asm"]
            if "OP_HASH160" in script:
                outs.append(("htlc", amount))
            elif "OP_CHECKSEQUENCEVERIFY" in script:
                if witness[-2] == "01":
                    outs.append(("penalty", amount))
                else:
                    outs.append(("balance", amount))
            else:
                # paying to a custom address, means the same as a pubkey
                # (it's anything the peer wants to spend to either in a
                #  mutual close or when the other party is force-closing)
                outs.append(("any", amount))

    # now that we have labels for all outputs we use a simple (maybe wrong?)
    # heuristic to determine what happened.
    if len(outs) == 1 and outs[0][0] == "any":
        typ = "unused"
        bal_a = int(outs[0][1])
    elif len(outs) == 2 and outs[0][0] == "any" and outs[1][0] == "any":
        typ = "mutual"
        bal_a = outs[0][1]
        bal_a = outs[1][1]
    else:
        for out, amt in outs:
            if out == "htlc":
                nhtlcs += 1
                typ = "force"
                continue

            if out == "penalty":
                typ = "penalty"

            if out == "balance":
                typ = "force"

            if out == "penalty" or out == "balance" or out == "any":
                if bal_a == 0:
                    bal_a = amt
                elif bal_b == 0:
                    bal_b = amt
                else:
                    raise ClosureTypeError("more than 2 balances")

    # no matter how balances were arranged above
    # if we detected a penalty we can assume everything is on one side
    if typ == "penalty":
        bal_a += bal_b
        bal_b = 0

    print(outs, end=" ")

    return typ, bal_a, bal_b, nhtlcs
