def schema(db):
    db.execute(
        """
CREATE TABLE IF NOT EXISTS channels (
  short_channel_id text PRIMARY KEY,
  open_block integer,
  open_time timestamp,
  open_transaction text,
  open_fee integer,
  address text,
  close_block integer,
  close_time timestamp,
  close_transaction text,
  close_fee integer,
  close_type text,
  close_htlc_count integer,
  close_balance_a integer,
  close_balance_b integer,
  node0 text NOT NULL,
  node1 text NOT NULL,
  satoshis integer,
  last_seen timestamp NOT NULL
)
    """
    )
    db.execute("GRANT SELECT ON channels TO web_anon")

    db.execute(
        """
CREATE TABLE IF NOT EXISTS nodealiases (
  pubkey text NOT NULL,
  alias text NOT NULL,
  first_seen timestamp NOT NULL,
  last_seen timestamp NOT NULL
)
    """
    )
    db.execute("GRANT SELECT ON nodealiases TO web_anon")

    db.execute(
        """
CREATE TABLE IF NOT EXISTS policies (
  short_channel_id text NOT NULL,
  direction integer NOT NULL, -- 1 means from node0 to node1 and vice-versa
  base_fee_millisatoshi numeric(13) NOT NULL,
  fee_per_millionth numeric(13) NOT NULL,
  delay integer NOT NULL,
  update_time timestamp NOT NULL
)
    """
    )
    db.execute("GRANT SELECT ON policies TO web_anon")

    db.execute("CREATE INDEX IF NOT EXISTS index_scid ON channels(short_channel_id)")
    db.execute("CREATE INDEX IF NOT EXISTS index_node0 ON channels(node0)")
    db.execute("CREATE INDEX IF NOT EXISTS index_node1 ON channels(node1)")
    db.execute("CREATE INDEX IF NOT EXISTS index_pubkey ON nodealiases(pubkey)")
