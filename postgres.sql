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
);

CREATE TABLE IF NOT EXISTS nodealiases (
  pubkey text NOT NULL,
  alias text NOT NULL,
  first_seen timestamp NOT NULL,
  last_seen timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
  short_channel_id text NOT NULL,
  direction integer NOT NULL, -- 1 means from node0 to node1 and vice-versa
  base_fee_millisatoshi numeric(13) NOT NULL,
  fee_per_millionth numeric(13) NOT NULL,
  delay integer NOT NULL,
  update_time timestamp NOT NULL
);

GRANT SELECT ON channels TO web_anon;
GRANT SELECT ON nodealiases TO web_anon;
GRANT SELECT ON policies TO web_anon;

CREATE INDEX IF NOT EXISTS index_scid ON channels(short_channel_id);
CREATE INDEX IF NOT EXISTS index_node0 ON channels(node0);
CREATE INDEX IF NOT EXISTS index_node1 ON channels(node1);
CREATE INDEX IF NOT EXISTS index_pubkey ON nodealiases(pubkey);

CREATE MATERIALIZED VIEW nodes AS
  WITH node AS (
    SELECT
      pubkey,
      (SELECT alias FROM nodealiases AS n WHERE nodealiases.pubkey = n.pubkey ORDER BY last_seen DESC LIMIT 1) AS alias
    FROM nodealiases
    GROUP BY pubkey
  ), open AS (
    SELECT pubkey, count(*) AS openchannels, sum(satoshis) AS capacity FROM (
      SELECT node0 AS pubkey, * FROM channels UNION ALL SELECT node1 AS pubkey, * FROM channels
    )x WHERE close_block IS NULL GROUP BY pubkey
  ), agg AS (
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
    )z GROUP BY pubkey
  )
  SELECT
    node.pubkey AS pubkey,
    node.alias AS alias,
    agg.oldestchannel AS oldestchannel,
    open.openchannels AS openchannels,
    agg.closedchannels AS closedchannels,
    open.capacity AS capacity,
    agg.avg_duration AS avg_duration,
    agg.avg_open_fee AS avg_open_fee,
    agg.avg_close_fee AS avg_close_fee
  FROM node
  INNER JOIN open ON open.pubkey = node.pubkey
  INNER JOIN agg ON agg.pubkey = node.pubkey;

CREATE MATERIALIZED VIEW globalstats AS
  WITH last_block AS (
    SELECT max(b) AS last_block
    FROM (
        SELECT max(open_block) AS b FROM channels
      UNION ALL
        SELECT max(close_block) AS b FROM channels
    )x
  ), channels AS (
    SELECT
      max(CASE
        WHEN close_block IS NOT NULL THEN close_block
        ELSE (SELECT last_block FROM last_block)
      END - open_block) AS max_duration,
      max(open_fee) AS max_open_fee,
      max(close_fee) AS max_close_fee,
      max(satoshis) AS max_satoshis
    FROM channels
  ), nodes AS (
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
  )
  SELECT
    (SELECT last_block FROM last_block) AS last_block,
    channels.max_duration       AS max_channel_duration,
    channels.max_open_fee       AS max_channel_open_fee,
    channels.max_close_fee      AS max_channel_close_fee,
    channels.max_satoshis       AS max_channel_satoshis,
    nodes.max_capacity          AS max_node_capacity,
    nodes.max_openchannels      AS max_node_openchannels,
    nodes.max_closedchannels    AS max_node_closedchannels,
    nodes.max_allchannels       AS max_node_allchannels,
    nodes.max_close_rate        AS max_node_close_rate,
    nodes.max_average_duration  AS max_node_average_duration,
    nodes.max_average_open_fee  AS max_node_average_open_fee,
    nodes.max_average_close_fee AS max_node_average_close_fee
  FROM channels, nodes;

CREATE MATERIALIZED VIEW closetypes AS
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
  SELECT
    blockgroup,
    coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'unknown'), 0) AS unknown,
    coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'unused'), 0) AS unused,
    coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'mutual'), 0) AS mutual,
    coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'force'), 0) AS force,
    coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'force unused'), 0) AS force_unused,
    coalesce((SELECT c FROM base WHERE base.blockgroup = b.blockgroup AND close_type = 'penalty'), 0) AS penalty
  FROM base AS b
  WHERE blockgroup IS NOT NULL
  GROUP BY blockgroup
  ORDER BY blockgroup;

GRANT SELECT ON nodes TO web_anon;
GRANT SELECT ON globalstats TO web_anon;
GRANT SELECT ON closetypes TO web_anon;

CREATE INDEX IF NOT EXISTS index_node ON nodes(pubkey);
