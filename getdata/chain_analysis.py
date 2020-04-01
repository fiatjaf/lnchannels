import json
import itertools


def chain_analysis(db):
    db.execute(
        """
SELECT * FROM (
  SELECT
    tx,
    count(*) AS c,
    array_agg(match) AS match,
    array_agg(short_channel_id) AS channels,
    bool_or(mistery) AS has_mistery
  FROM (
      SELECT
        short_channel_id,
        'a' AS match,
        (onchain->'a' = 'null' OR onchain->'open'->'funder' = 'null') AS mistery,
        jsonb_array_elements_text(onchain->'txs'->'a') AS tx
      FROM channels
    UNION ALL
      SELECT
        short_channel_id,
        'b' AS match,
        (onchain->'a' = 'null' OR onchain->'open'->'funder' = 'null') AS mistery,
        jsonb_array_elements_text(onchain->'txs'->'b') AS tx
      FROM channels
    UNION ALL
      SELECT
        short_channel_id,
        'funding' AS match,
        (onchain->'a' = 'null' OR onchain->'open'->'funder' = 'null') AS mistery,
        jsonb_array_elements_text(onchain->'txs'->'funding') AS tx
      FROM channels
  )x
  GROUP BY tx
)y
WHERE c > 1 AND has_mistery
    """
    )
    for _, _, match, channels, _ in db.fetchall():
        for scid1, scid2 in itertools.combinations(channels, 2):
            print("chain analysis:", scid1, scid2, " ", match)
            chain_analysis_for(db, scid1, scid2, set(match))


def chain_analysis_for(db, scid1, scid2, match_summary):
    db.execute(
        "SELECT onchain, nodes FROM channels WHERE short_channel_id = %s", (scid1,)
    )
    data1, nodes1 = db.fetchone()

    db.execute(
        "SELECT onchain, nodes FROM channels WHERE short_channel_id = %s", (scid2,)
    )
    data2, nodes2 = db.fetchone()

    # matching helpers
    updated = False
    m = Matcher((nodes1, data1), (nodes2, data2))

    # if the channels are between the same people we can't know anything, probably
    if nodes1 == nodes2:
        # will decide what to do later
        return

    # in case of penalty first we set 'a' and 'b' to the same
    if (
        data1["close"].get("type") == "penalty"
        or data2["close"].get("type") == "penalty"
    ):
        # will decide what to do later
        return

    # match nodes sharing outputs after channel closes
    if match_summary != {"funding"} and (
        data1["a"],
        data2["a"],
        data1["b"],
        data2["b"],
    ) != (None, None, None, None):
        updated = True
        if m.matches("a", "a"):
            common = set(nodes1).intersection(set(nodes2)).pop()
            data1["a"] = nodes1.index(common)
            data2["a"] = nodes2.index(common)
            data1["b"] = 1 - data1["a"]
            data2["b"] = 1 - data2["a"]
        if m.matches("b", "b"):
            common = set(nodes1).intersection(set(nodes2)).pop()
            data1["b"] = nodes1.index(common)
            data2["b"] = nodes2.index(common)
            data1["a"] = 1 - data1["b"]
            data2["a"] = 1 - data2["b"]
        if m.matches("a", "b"):
            common = set(nodes1).intersection(set(nodes2)).pop()
            data1["a"] = nodes1.index(common)
            data2["b"] = nodes2.index(common)
            data1["b"] = 1 - data1["a"]
            data2["a"] = 1 - data2["b"]
        if m.matches("b", "a"):
            common = set(nodes1).intersection(set(nodes2)).pop()
            data1["b"] = nodes1.index(common)
            data2["a"] = nodes2.index(common)
            data1["a"] = 1 - data1["b"]
            data2["b"] = 1 - data2["a"]

    # match nodes sharing inputs with outputs across channels
    if "funding" in match_summary and len(match_summary) > 1:
        updated = True
        try:
            if m.matches("a", "funding"):
                common = set(nodes1).intersection(set(nodes2)).pop()
                data1["a"] = nodes1.index(common)
                data1["b"] = 1 - data1["a"]
                data2["funder"] = nodes2.index(common)
            if m.matches("b", "funding"):
                common = set(nodes1).intersection(set(nodes2)).pop()
                data1["b"] = nodes1.index(common)
                data1["a"] = 1 - data1["b"]
                data2["funder"] = nodes2.index(common)
            if m.matches("funding", "a"):
                common = set(nodes1).intersection(set(nodes2)).pop()
                data2["a"] = nodes2.index(common)
                data2["b"] = 1 - data2["a"]
                data1["funder"] = nodes1.index(common)
            if m.matches("funding", "b"):
                common = set(nodes1).intersection(set(nodes2)).pop()
                data2["b"] = nodes2.index(common)
                data2["a"] = 1 - data2["b"]
                data1["funder"] = nodes1.index(common)
        except KeyError:
            # funding to two different nodes may come from the same transaction
            # since we're not tracking output number an error may happen here.
            pass

    # if we know the funder of two channels is the same we know who it is
    if m.matches("funding", "funding"):
        try:
            funder_id = set(nodes1).intersection(set(nodes2)).pop()
            data1["funder"] = nodes1.index(funder_id)
            data2["funder"] = nodes2.index(funder_id)
            updated = True
        except KeyError:
            # funding to two different nodes may come from the same transaction
            # since we're not tracking output number an error may happen here.
            pass

    # match the channel funder with known nodes1
    # funder here will be set to either 0 or 1
    if "funding" in match_summary and len(match_summary) > 2:
        updated = True
        if m.matches("funding", "a") and data1["a"]:
            data1["funder"] = nodes1.index(nodes2[data2["a"]])
        if m.matches("funding", "b") and data1["b"]:
            data1["funder"] = nodes1.index(nodes2[data2["b"]])
        if m.matches("a", "funding",) and data2["a"]:
            data2["funder"] = nodes2.index(nodes1[data1["a"]])
        if m.matches("b", "funding",) and data2["b"]:
            data2["funder"] = nodes2.index(nodes1[data1["b"]])

    # if the channel only has one close balance we automatically know things
    for data in [data1, data2]:
        if data["close"]["balance"]["b"] == 0:
            updated = True
            # 'a' is the closer.
            data["closer"] = "a"
            # if we know the funder we apply that to 'a'
            if data["funder"]:
                data["a"] = data["funder"]
                data["b"] = 1 - data["a"]
            # if we know 'a' we apply that to the funder
            if data["a"]:
                data["funder"] = data["a"]

    if not updated:
        return

    for scid, data in [(scid1, data1), (scid2, data2)]:
        print(
            f'  result: {scid}, a: {data["a"]}, b: {data["b"]}, ({data["funder"]}->{data["closer"]})'
        )

        db.execute(
            """
UPDATE channels
SET onchain = %s
WHERE short_channel_id = %s
        """,
            (json.dumps(data), scid,),
        )


class Matcher:
    def __init__(self, stuff1, stuff2):
        nodes1, data1 = stuff1
        nodes2, data2 = stuff2

        self.txs = {
            1: {
                "a": set(data1["txs"]["a"]),
                "b": set(data1["txs"]["b"]),
                "funding": set(data1["txs"]["funding"]),
            },
            2: {
                "a": set(data2["txs"]["a"]),
                "b": set(data2["txs"]["b"]),
                "funding": set(data2["txs"]["funding"]),
            },
        }

    def matches(self, tag1, tag2):
        return self.txs[1][tag1].intersection(self.txs[2][tag2])
