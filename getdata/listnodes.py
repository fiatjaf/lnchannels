import os
import requests

SPARK_URL = os.getenv("SPARK_URL")
SPARK_TOKEN = os.getenv("SPARK_TOKEN")


def listnodes(db):
    r = requests.get(
        SPARK_URL, headers={"X-Access": SPARK_TOKEN}, json={"method": "listnodes"}
    )
    for node in r.json()["nodes"]:
        c = db.execute(
            """
SELECT last_seen FROM (
    SELECT last_seen, pubkey, alias
    FROM nodealiases
    WHERE pubkey = ?
    ORDER BY last_seen DESC
    LIMIT 1
) WHERE alias = ?
        """,
            (node["nodeid"], node["alias"]),
        )
        row = c.fetchone()
        if row:
            last_seen = row[0]
            db.execute(
                """
INSERT INTO nodealiases (pubkey, alias, first_seen, last_seen)
VALUES (?, ?, datetime('now'), datetime('now'))
            """,
                (last_seen, node["nodeid"]),
            )
        else:
            db.execute(
                """
UPDATE nodealiases
SET last_seen = datetime('now')
WHERE last_seen = ? AND pubkey = ?
            """,
                (node["nodeid"], node["alias"]),
            )

        print("inserted", node["nodeid"])
