import requests

from .globals import SPARK_URL, SPARK_TOKEN


def listnodes(db):
    r = requests.post(
        SPARK_URL, headers={"X-Access": SPARK_TOKEN}, json={"method": "listnodes"}
    )
    for node in r.json()["nodes"]:
        db.execute(
            """
SELECT last_seen FROM (
    SELECT last_seen, pubkey, alias
    FROM nodealiases
    WHERE pubkey = %s
    ORDER BY last_seen DESC
    LIMIT 1
)x WHERE alias = %s
        """,
            (node["nodeid"], node.get("alias", "")),
        )
        row = db.fetchone()

        # if this alias is already registered just update its last_seen timestamp
        # if it's a new alias for this node, add a new row with it

        if row:
            last_seen = row[0]
            db.execute(
                """
UPDATE nodealiases
SET
  last_seen = now(),
  color = %s
WHERE last_seen = %s AND pubkey = %s
            """,
                (node.get("color", ""), last_seen, node["nodeid"]),
            )
        else:
            db.execute(
                """
INSERT INTO nodealiases
  (pubkey, color, alias, first_seen, last_seen)
VALUES (%s, %s, %s, now(), now())
            """,
                (node["nodeid"], node.get("color", ""), node.get("alias", "")),
            )

        print("inserted", node["nodeid"])
