def schema(db):
    db.execute(
        """
CREATE TABLE IF NOT EXISTS channels (
  short_channel_id TEXT PRIMARY KEY,
  open_block INTEGER,
  open_time DATETIME,
  open_transaction TEXT,
  open_fee INTEGER,
  address TEXT,
  close_block INTEGER,
  close_time DATETIME,
  close_transaction TEXT,
  lose_fee INTEGER,
  close_type TEXT,
  close_htlc_count INTEGER,
  close_balance_a INTEGER, -- we don't know if node0 is A or B
  close_balance_b INTEGER, -- and vice-versa
  node0 TEXT NOT NULL,
  node1 TEXT NOT NULL,
  satoshis INTEGER,
  last_update INTEGER,
  last_seen DATETIME NOT NULL
)
    """
    )
    db.execute(
        """
CREATE TABLE IF NOT EXISTS nodealiases (
  pubkey TEXT NOT NULL,
  alias TEXT NOT NULL,
  first_seen DATETIME NOT NULL,
  last_seen DATETIME NOT NULL
)
    """
    )

    db.execute(
        """
CREATE TABLE IF NOT EXISTS policies (
  short_channel_id TEXT NOT NULL,
  direction INTEGER NOT NULL, -- 1 means from node0 to node1 and vice-versa
  base_fee_millisatoshi INTEGER NOT NULL,
  fee_per_millionth INTEGER NOT NULL,
  delay INTEGER NOT NULL,
  
  update_time INTEGER NOT NULL
)
    """
    )

    db.execute("CREATE INDEX IF NOT EXISTS index_scid ON channels(short_channel_id)")
    db.execute("CREATE INDEX IF NOT EXISTS index_node0 ON channels(node0)")
    db.execute("CREATE INDEX IF NOT EXISTS index_node1 ON channels(node1)")
    db.execute("CREATE INDEX IF NOT EXISTS index_pubkey ON nodealiases(pubkey)")
