def materialize(db):
    db.execute(
        """
CREATE TABLE IF NOT EXISTS nodes (
  pubkey TEXT PRIMARY KEY,
  oldestchannel INTEGER NOT NULL,
  openchannels INTEGER NOT NULL,
  closedchannels INTEGER NOT NULL,
  capacity INTEGER NOT NULL,
  avg_duration INTEGER NOT NULL,
  avg_open_fee INTEGER NOT NULL,
  avg_close_fee INTEGER,
  alias TEXT
)
    """
    )
    db.execute(
        """
CREATE TABLE IF NOT EXISTS globalstats (
  last_block INTEGER NOT NULL,
  max_channel_duration INTEGER NOT NULL,
  max_channel_open_fee INTEGER NOT NULL,
  max_channel_close_fee INTEGER NOT NULL,
  max_channel_satoshis INTEGER NOT NULL,
  max_node_capacity INTEGER NOT NULL,
  max_node_openchannels INTEGER NOT NULL,
  max_node_closedchannels INTEGER NOT NULL,
  max_node_allchannels INTEGER NOT NULL,
  max_node_close_rate INTEGER NOT NULL,
  max_node_average_duration INTEGER NOT NULL,
  max_node_average_open_fee INTEGER NOT NULL,
  max_node_average_close_fee INTEGER NOT NULL
)
    """
    )
    db.execute(
        """
CREATE TABLE IF NOT EXISTS closetypes (
  blockgroup INTEGER NOT NULL,
  unknown INTEGER NOT NULL,
  unused INTEGER NOT NULL,
  mutual INTEGER NOT NULL,
  force INTEGER NOT NULL,
  force_unused INTEGER NOT NULL,
  penalty INTEGER NOT NULL
)
    """
    )
    db.execute("DELETE FROM nodes")
    db.execute("DELETE FROM globalstats")
    db.execute("DELETE FROM closetypes")
    db.execute(
        """
INSERT INTO nodes
  (pubkey, alias, oldestchannel,
    openchannels, closedchannels, capacity,
    avg_duration, avg_open_fee, avg_close_fee)
SELECT
  n.pubkey, n.alias, agg.oldestchannel,
    open.openchannels, agg.closedchannels, open.capacity,
    agg.avg_duration, agg.avg_open_fee, agg.avg_close_fee
FROM nodealiases AS n
INNER JOIN (
  SELECT pubkey, count(*) AS openchannels, sum(satoshis) AS capacity FROM (
    SELECT node0 AS pubkey, * FROM channels UNION ALL SELECT node1 AS pubkey, * FROM channels
  ) WHERE close_block IS NULL GROUP BY pubkey
) AS open ON open.pubkey = n.pubkey
INNER JOIN (
  SELECT pubkey,
    min(open_block) AS oldestchannel,
    count(close_block) AS closedchannels,
    avg(CASE WHEN close_block IS NOT NULL
      THEN close_block
      ELSE (SELECT open_block FROM channels ORDER BY open_block DESC LIMIT 1)
    END - open_block) AS avg_duration,
    avg(open_fee) AS avg_open_fee,
    avg(close_fee) AS avg_close_fee
  FROM (
    SELECT node0 AS pubkey, * FROM channels UNION ALL SELECT node1 AS pubkey, * FROM channels
  ) GROUP BY pubkey
) AS agg ON agg.pubkey = n.pubkey
GROUP BY n.pubkey
ORDER BY n.last_seen
    """
    )
    db.execute(
        """
WITH last_block AS (
  SELECT max(b) AS last_block
  FROM (
      SELECT max(open_block) AS b FROM channels
    UNION ALL
      SELECT max(close_block) AS b FROM channels
  )
)
INSERT INTO globalstats
SELECT
  (SELECT last_block FROM last_block), -- last_block
  channels.max_duration      , -- max_channel_duration
  channels.max_open_fee      , -- max_channel_open_fee
  channels.max_close_fee     , -- max_channel_close_fee
  channels.max_satoshis      , -- max_channel_satoshis
  nodes.max_capacity         , -- max_node_capacity
  nodes.max_openchannels     , -- max_node_openchannels
  nodes.max_closedchannels   , -- max_node_closedchannels
  nodes.max_allchannels      , -- max_node_allchannels
  nodes.max_close_rate       , -- max_node_close_rate
  nodes.max_average_duration , -- max_node_average_duration
  nodes.max_average_open_fee , -- max_node_average_open_fee
  nodes.max_average_close_fee  -- max_node_average_close_fee
FROM (
  SELECT
    max(CASE
      WHEN close_block IS NOT NULL THEN close_block
      ELSE (SELECT last_block FROM last_block)
    END - open_block) AS max_duration,
    max(open_fee) AS max_open_fee,
    max(close_fee) AS max_close_fee,
    max(satoshis) AS max_satoshis
  FROM channels
) AS channels
JOIN (
  SELECT
    max(capacity) AS max_capacity,
    max(openchannels) AS max_openchannels,
    max(closedchannels) AS max_closedchannels,
    max(openchannels + closedchannels) AS max_allchannels,
    max(closedchannels / openchannels) AS max_close_rate,
    max(avg_duration) AS max_average_duration,
    max(avg_open_fee) AS max_average_open_fee,
    max(avg_close_fee) AS max_average_close_fee
  FROM nodes
) AS nodes
    """
    )
    db.execute(
        """
WITH dchannels AS (
  SELECT
    close_block,
    CASE WHEN close_type = 'force' AND close_balance_b = 0 THEN 'force unused' ELSE close_type END AS close_type
  FROM channels
), base AS (
  SELECT (close_block/1000)*1000 AS blockgroup,
    close_type,
    count(close_type) AS c
  FROM dchannels
  GROUP BY close_block/1000, close_type
)
INSERT INTO closetypes
SELECT blockgroup,
  coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'unknown'), 0),
  coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'unused'), 0),
  coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'mutual'), 0),
  coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'force'), 0),
  coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'force unused'), 0),
  coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'penalty'), 0)
FROM base AS b
WHERE blockgroup IS NOT NULL
GROUP BY blockgroup
ORDER BY blockgroup
    """
    )
