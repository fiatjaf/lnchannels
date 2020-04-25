import requests
from tqdm import tqdm

from .globals import SPARK_URL, SPARK_TOKEN


def listnodes(db):
    r = requests.post(
        SPARK_URL, headers={"X-Access": SPARK_TOKEN}, json={"method": "listnodes"}
    )
    nodes = r.json()["nodes"]

    db.execute(
        """
SELECT DISTINCT ON (pubkey) pubkey, alias
FROM nodealiases
ORDER BY pubkey, first_seen DESC
        """
    )
    nodealiases = {pubkey: alias for pubkey, alias in db.fetchall()}
    db.execute(
        """
SELECT DISTINCT ON (pubkey) pubkey, features
FROM features
ORDER BY pubkey, first_seen DESC
        """
    )
    nodefeatures = {pubkey: features for pubkey, features in db.fetchall()}

    for node in tqdm(nodes, leave=True, desc="listnodes"):
        pubkey = node["nodeid"]

        # alias, color
        alias = node.get("alias")
        if alias and nodealiases.get(pubkey) != alias:
            db.execute(
                """
INSERT INTO nodealiases
  (pubkey, color, alias, first_seen)
VALUES (%s, %s, %s, now())
            """,
                (pubkey, node.get("color", ""), alias),
            )

        # features bitstring
        features = node.get("features")
        if features and nodefeatures.get(pubkey) != features:
            db.execute(
                """
INSERT INTO features
  (pubkey, features, first_seen)
VALUES (%s, %s, now())
            """,
                (pubkey, features),
            )
